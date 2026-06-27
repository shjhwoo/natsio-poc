package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

const (
	serviceAName = "SERVICEA"
	userName     = "servicea-user"
)

type state struct {
	OperatorSeed string `json:"operator_seed"`
	AccountSeed  string `json:"account_seed"`
	AccountPub   string `json:"account_pub"`
	AccountJWT   string `json:"account_jwt"`
}

type result struct {
	Name       string
	Expected   string
	Actual     string
	Pass       bool
	Conclusion string
}

type probeConn struct {
	nc   *nats.Conn
	errC chan string
	mu   sync.Mutex
}

func main() {
	log.SetFlags(0)
	prepareOnly := flag.Bool("prepare", false, "generate auth fixtures and config files")
	topology := flag.String("topology", "single", "single or cluster")
	flag.Parse()

	if *prepareOnly {
		must(prepare(), "prepare")
		log.Println("prepare complete")
		return
	}

	results := run(*topology)
	log.Println("\n" + strings.Repeat("=", 78))
	log.Printf("결과 요약 (%s)", *topology)
	log.Println(strings.Repeat("=", 78))
	failed := 0
	for _, r := range results {
		mark := "✗"
		if r.Pass {
			mark = "✓"
		} else {
			failed++
		}
		log.Printf("%s %-30s expected=%-28s actual=%-38s %s", mark, r.Name, r.Expected, r.Actual, r.Conclusion)
	}
	if failed > 0 {
		os.Exit(1)
	}
}

func prepare() error {
	mustMkdir(filepath.Join("generated", "auth", "operator"))
	mustMkdir(filepath.Join("generated", "creds"))

	operatorKP, err := nkeys.CreateOperator()
	if err != nil {
		return err
	}
	operatorPub, err := operatorKP.PublicKey()
	if err != nil {
		return err
	}
	operatorClaims := jwt.NewOperatorClaims(operatorPub)
	operatorClaims.Name = "POC-OPERATOR"
	operatorJWT, err := operatorClaims.Encode(operatorKP)
	if err != nil {
		return err
	}
	operatorSeed, err := operatorKP.Seed()
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join("generated", "auth", "operator", "operator.jwt"), []byte(operatorJWT), 0o644); err != nil {
		return err
	}

	accountKP, err := nkeys.CreateAccount()
	if err != nil {
		return err
	}
	accountPub, err := accountKP.PublicKey()
	if err != nil {
		return err
	}
	accountClaims := jwt.NewAccountClaims(accountPub)
	accountClaims.Name = serviceAName
	accountJWT, err := accountClaims.Encode(operatorKP)
	if err != nil {
		return err
	}
	accountSeed, err := accountKP.Seed()
	if err != nil {
		return err
	}

	st := state{
		OperatorSeed: string(operatorSeed),
		AccountSeed:  string(accountSeed),
		AccountPub:   accountPub,
		AccountJWT:   accountJWT,
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join("generated", "state.json"), b, 0o644); err != nil {
		return err
	}

	v1, err := createUserCreds(st.AccountSeed, userName, []string{"servicea.base"}, []string{"servicea.base"})
	if err != nil {
		return err
	}
	if err := os.WriteFile(credsPath("v1"), []byte(v1), 0o644); err != nil {
		return err
	}

	if err := writeSingleConfig(st); err != nil {
		return err
	}
	if err := writeClusterConfigs(st); err != nil {
		return err
	}
	return nil
}

func run(topology string) []result {
	st := mustLoadState()
	var targets []string
	representative := "nats://127.0.0.1:4222"
	switch topology {
	case "single":
		targets = []string{"nats://127.0.0.1:4222"}
		representative = targets[0]
	case "cluster":
		targets = []string{"nats://127.0.0.1:4223", "nats://127.0.0.1:4224", "nats://127.0.0.1:4225"}
		representative = targets[0]
	default:
		must(fmt.Errorf("unknown topology %s", topology), "topology")
	}

	waitForTargets(targets, credsPath("v1"))
	log.Printf("=== User 권한 추가/삭제 runtime PoC (%s) ===", topology)
	log.Printf("nats.go: %s", natsGoVersion())

	var results []result

	// Baseline on all targets with v1
	for i, url := range targets {
		label := fmt.Sprintf("node%d baseline", i+1)
		pass, actual := runBaseline(url, credsPath("v1"), "v1")
		results = append(results, result{
			Name:       label,
			Expected:   "base allow extra deny",
			Actual:     actual,
			Pass:       pass,
			Conclusion: "기본 권한 상태 확인",
		})
	}

	// Existing representative v1 connection kept alive
	oldV1 := mustConnect(representative, credsPath("v1"), "existing-v1")
	defer oldV1.close()

	// Add permission -> v2
	v2 := mustCreateUserCreds(st.AccountSeed, userName, []string{"servicea.base", "servicea.extra"}, []string{"servicea.base", "servicea.extra"})
	must(os.WriteFile(credsPath("v2"), []byte(v2), 0o644), "write v2 creds")

	passOldAfterAdd, actualOldAfterAdd := probeMatrix(oldV1, true, false)
	results = append(results, result{
		Name:       "existing v1 after add",
		Expected:   "base allow extra still deny",
		Actual:     actualOldAfterAdd,
		Pass:       passOldAfterAdd,
		Conclusion: "기존 연결은 자동 재권한평가되지 않을 수 있음",
	})

	var repV2 *probeConn
	for i, url := range targets {
		pc := mustConnect(url, credsPath("v2"), fmt.Sprintf("v2-node%d", i+1))
		if i == 0 {
			repV2 = pc
		} else {
			defer pc.close()
		}
		pass, actual := probeMatrix(pc, true, true)
		results = append(results, result{
			Name:       fmt.Sprintf("node%d new v2 after add", i+1),
			Expected:   "base allow extra allow",
			Actual:     actual,
			Pass:       pass,
			Conclusion: "새 연결은 restart/reload 없이 추가 권한 사용 가능해야 함",
		})
	}
	defer repV2.close()

	// Remove base permission -> v3 (extra only)
	v3 := mustCreateUserCreds(st.AccountSeed, userName, []string{"servicea.extra"}, []string{"servicea.extra"})
	must(os.WriteFile(credsPath("v3"), []byte(v3), 0o644), "write v3 creds")

	passOldAfterRemove, actualOldAfterRemove := probeMatrix(repV2, true, true)
	results = append(results, result{
		Name:       "existing v2 after remove",
		Expected:   "base still allow extra allow",
		Actual:     actualOldAfterRemove,
		Pass:       passOldAfterRemove,
		Conclusion: "이미 연결된 세션은 삭제된 권한을 계속 가질 수 있음",
	})

	for i, url := range targets {
		pc := mustConnect(url, credsPath("v3"), fmt.Sprintf("v3-node%d", i+1))
		defer pc.close()
		pass, actual := probeMatrix(pc, false, true)
		results = append(results, result{
			Name:       fmt.Sprintf("node%d new v3 after remove", i+1),
			Expected:   "base deny extra allow",
			Actual:     actual,
			Pass:       pass,
			Conclusion: "새 연결은 restart/reload 없이 삭제된 권한 상태를 반영해야 함",
		})
	}

	return results
}

func runBaseline(url, creds, name string) (bool, string) {
	pc := mustConnect(url, creds, name)
	defer pc.close()
	return probeMatrix(pc, true, false)
}

func probeMatrix(pc *probeConn, expectBase, expectExtra bool) (bool, string) {
	basePub := pc.tryPublish("servicea.base")
	extraPub := pc.tryPublish("servicea.extra")
	baseSub := pc.trySubscribe("servicea.base")
	extraSub := pc.trySubscribe("servicea.extra")
	actual := fmt.Sprintf("pub(base=%s extra=%s) sub(base=%s extra=%s)", outcome(basePub), outcome(extraPub), outcome(baseSub), outcome(extraSub))
	pass := basePub == expectBase && extraPub == expectExtra && baseSub == expectBase && extraSub == expectExtra
	return pass, actual
}

func outcome(ok bool) string {
	if ok {
		return "allow"
	}
	return "deny"
}

func mustConnect(url, creds, name string) *probeConn {
	errC := make(chan string, 32)
	nc, err := nats.Connect(url,
		nats.UserCredentials(creds),
		nats.Name(name),
		nats.NoReconnect(),
		nats.Timeout(2*time.Second),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			select {
			case errC <- err.Error():
			default:
			}
		}),
	)
	must(err, "connect "+url+" as "+name)
	must(nc.FlushTimeout(2*time.Second), "flush connect "+name)
	return &probeConn{nc: nc, errC: errC}
}

func (p *probeConn) close() {
	if p.nc != nil {
		p.nc.Close()
	}
}

func (p *probeConn) drainErrors() {
	for {
		select {
		case <-p.errC:
		default:
			return
		}
	}
}

func (p *probeConn) tryPublish(subject string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.drainErrors()
	must(p.nc.Publish(subject, []byte("probe")), "publish issue")
	must(p.nc.FlushTimeout(2*time.Second), "publish flush")
	return !p.hasPermissionViolation(250 * time.Millisecond)
}

func (p *probeConn) trySubscribe(subject string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.drainErrors()
	sub, err := p.nc.SubscribeSync(subject)
	must(err, "subscribe issue")
	must(p.nc.FlushTimeout(2*time.Second), "subscribe flush")
	_ = sub.Unsubscribe()
	_ = p.nc.FlushTimeout(2 * time.Second)
	return !p.hasPermissionViolation(250 * time.Millisecond)
}

func (p *probeConn) hasPermissionViolation(wait time.Duration) bool {
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	for {
		select {
		case msg := <-p.errC:
			if strings.Contains(strings.ToLower(msg), "permission") {
				return true
			}
		case <-deadline.C:
			return false
		}
	}
}

func waitForTargets(targets []string, creds string) {
	deadline := time.Now().Add(20 * time.Second)
	remaining := map[string]bool{}
	for _, t := range targets {
		remaining[t] = true
	}
	for time.Now().Before(deadline) {
		for _, t := range targets {
			if !remaining[t] {
				continue
			}
			nc, err := nats.Connect(t, nats.UserCredentials(creds), nats.NoReconnect(), nats.Timeout(1*time.Second))
			if err == nil {
				nc.Close()
				remaining[t] = false
			}
		}
		allDone := true
		for _, done := range remaining {
			if done {
				allDone = false
				break
			}
		}
		if allDone {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	must(fmt.Errorf("targets not ready: %v", remaining), "waitForTargets")
}

func writeSingleConfig(st state) error {
	cfg := fmt.Sprintf("port: 4222\nhttp_port: 8222\nserver_name: n1\noperator: \"/etc/nats/auth/operator/operator.jwt\"\nresolver: MEMORY\nresolver_preload: {\n  \"%s\": \"%s\"\n}\n", st.AccountPub, st.AccountJWT)
	return os.WriteFile("nats.single.conf", []byte(cfg), 0o644)
}

func writeClusterConfigs(st state) error {
	configs := map[string]string{
		"n1.conf": clusterConfig("n1", 4222, 8222, 6222, []string{"nats://n2:6222", "nats://n3:6222"}, st),
		"n2.conf": clusterConfig("n2", 4222, 8222, 6222, []string{"nats://n1:6222", "nats://n3:6222"}, st),
		"n3.conf": clusterConfig("n3", 4222, 8222, 6222, []string{"nats://n1:6222", "nats://n2:6222"}, st),
	}
	for path, body := range configs {
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func clusterConfig(name string, port, httpPort, clusterPort int, routes []string, st state) string {
	quotedRoutes := make([]string, 0, len(routes))
	for _, r := range routes {
		quotedRoutes = append(quotedRoutes, fmt.Sprintf("    \"%s\"", r))
	}
	return fmt.Sprintf("server_name: %s\nport: %d\nhttp_port: %d\ncluster {\n  name: \"AUTH-POC\"\n  listen: 0.0.0.0:%d\n  routes = [\n%s\n  ]\n}\noperator: \"/etc/nats/auth/operator/operator.jwt\"\nresolver: MEMORY\nresolver_preload: {\n  \"%s\": \"%s\"\n}\n", name, port, httpPort, clusterPort, strings.Join(quotedRoutes, ",\n"), st.AccountPub, st.AccountJWT)
}

func createUserCreds(accountSeed, user string, pubAllow, subAllow []string) (string, error) {
	accountKP, err := nkeys.FromSeed([]byte(accountSeed))
	if err != nil {
		return "", err
	}
	userKP, err := nkeys.CreateUser()
	if err != nil {
		return "", err
	}
	userPub, err := userKP.PublicKey()
	if err != nil {
		return "", err
	}
	uc := jwt.NewUserClaims(userPub)
	uc.Name = user
	for _, s := range pubAllow {
		uc.Permissions.Pub.Allow.Add(s)
	}
	for _, s := range subAllow {
		uc.Permissions.Sub.Allow.Add(s)
	}
	userJWT, err := uc.Encode(accountKP)
	if err != nil {
		return "", err
	}
	seed, err := userKP.Seed()
	if err != nil {
		return "", err
	}
	creds, err := jwt.FormatUserConfig(userJWT, seed)
	if err != nil {
		return "", err
	}
	return string(creds), nil
}

func mustCreateUserCreds(accountSeed, user string, pubAllow, subAllow []string) string {
	c, err := createUserCreds(accountSeed, user, pubAllow, subAllow)
	must(err, "create creds")
	return c
}

func credsPath(version string) string {
	return filepath.Join("generated", "creds", fmt.Sprintf("%s-%s.creds", userName, version))
}

func mustLoadState() state {
	b, err := os.ReadFile(filepath.Join("generated", "state.json"))
	must(err, "read state")
	var st state
	must(json.Unmarshal(b, &st), "parse state")
	return st
}

func mustMkdir(path string) {
	must(os.MkdirAll(path, 0o755), "mkdir")
}

func must(err error, msg string) {
	if err == nil {
		return
	}
	log.Fatalf("%s: %v", msg, err)
}

func natsGoVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dep := range info.Deps {
		if dep.Path == "github.com/nats-io/nats.go" {
			return dep.Version
		}
	}
	return "unknown"
}
