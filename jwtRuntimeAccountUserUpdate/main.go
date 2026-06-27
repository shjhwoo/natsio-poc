package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

const (
	natsURL       = "nats://127.0.0.1:4222"
	serviceAName  = "SERVICEA"
	serviceBName  = "SERVICEB"
	serviceAUser1 = "servicea-user"
	serviceAUser2 = "servicea-user-2"
	serviceBUser1 = "serviceb-user"
)

type result struct {
	Name       string
	Expected   string
	Actual     string
	Pass       bool
	Conclusion string
}

type state struct {
	OperatorSeed string        `json:"operator_seed"`
	ServiceA     accountRecord `json:"service_a"`
}

type accountRecord struct {
	Name         string   `json:"name"`
	AccountSeed  string   `json:"account_seed"`
	AccountPub   string   `json:"account_pub"`
	AccountJWT   string   `json:"account_jwt"`
	AllowedPub   []string `json:"allowed_pub"`
	AllowedSub   []string `json:"allowed_sub"`
	User1Creds   string   `json:"user1_creds"`
	User1Name    string   `json:"user1_name"`
	SubjectProbe string   `json:"subject_probe"`
}

type generatedAccount struct {
	Name         string
	AccountSeed  string
	AccountPub   string
	AccountJWT   string
	AllowedPub   []string
	AllowedSub   []string
	UserName     string
	UserCreds    string
	SubjectProbe string
}

func main() {
	log.SetFlags(0)
	prepareOnly := flag.Bool("prepare", false, "generate operator/account/user artifacts and initial nats.conf")
	flag.Parse()

	if *prepareOnly {
		must(prepare(), "prepare")
		log.Println("prepare complete")
		return
	}

	results := mustRun()
	log.Println("\n" + strings.Repeat("=", 78))
	log.Println("결과 요약")
	log.Println(strings.Repeat("=", 78))
	failed := 0
	for _, r := range results {
		mark := "✗"
		if r.Pass {
			mark = "✓"
		} else {
			failed++
		}
		log.Printf("%s %-24s expected=%-24s actual=%-32s %s", mark, r.Name, r.Expected, r.Actual, r.Conclusion)
	}
	if failed > 0 {
		os.Exit(1)
	}
}

func mustRun() []result {
	st := mustLoadState()
	serviceB := mustCreateAccount(serviceBName, serviceBUser1, []string{"_INBOX.>", "serviceb.>", "svc.echo"}, []string{"_INBOX.>", "serviceb.>", "svc.echo"})
	mustWriteString(credsPath(serviceBUser1), serviceB.UserCreds)

	log.Println("=== 운영 중 신규 Account/User 추가 시 서버 재시작 필요 여부 PoC ===")
	log.Printf("NATS Server target: %s", natsURL)
	log.Printf("nats.go: %s", natsGoVersion())

	var results []result

	// Phase A
	connA1, err := connectWithCreds(st.ServiceA.User1Creds, serviceAUser1)
	must(err, "baseline servicea-user connect")
	defer connA1.Close()
	must(connA1.Publish(st.ServiceA.SubjectProbe, []byte("baseline")), "baseline publish")
	must(connA1.FlushTimeout(2*time.Second), "baseline flush")
	results = append(results, result{
		Name:       "Phase A baseline",
		Expected:   "SERVICEA user connect ok",
		Actual:     "connect ok + publish ok",
		Pass:       true,
		Conclusion: "preload된 account/user는 정상 접속",
	})

	// Phase B
	serviceAUser2Creds := mustCreateUserCreds(st.ServiceA.AccountSeed, serviceAUser2, st.ServiceA.AllowedPub, st.ServiceA.AllowedSub)
	mustWriteString(credsPath(serviceAUser2), serviceAUser2Creds)
	connA2, err := connectWithCreds(credsPath(serviceAUser2), serviceAUser2)
	passB := err == nil
	actualB := "connect failed"
	if passB {
		defer connA2.Close()
		must(connA2.Publish(st.ServiceA.SubjectProbe, []byte("user2")), "user2 publish")
		must(connA2.FlushTimeout(2*time.Second), "user2 flush")
		actualB = "connect ok without reload"
	}
	results = append(results, result{
		Name:       "Phase B existing account new user",
		Expected:   "connect without reload",
		Actual:     actualB,
		Pass:       passB,
		Conclusion: "기존 account를 서버가 이미 신뢰하면 새 user는 즉시 검증 가능해야 함",
	})

	// Phase C
	_, err = connectWithCreds(credsPath(serviceBUser1), serviceBUser1)
	passC := err != nil
	actualC := "unexpected connect success"
	if err != nil {
		actualC = err.Error()
	}
	results = append(results, result{
		Name:       "Phase C new account before reload",
		Expected:   "connect fails",
		Actual:     actualC,
		Pass:       passC,
		Conclusion: "resolver_preload에 없는 account는 인증 불가",
	})

	// Phase D/E
	must(mustWriteConfig(st.ServiceA, accountRecordFromGenerated(serviceB)), "write config with SERVICEB")
	containerIDBefore := mustDockerComposePSQ("nats-server")
	startedBefore := mustDockerInspectStartedAt(containerIDBefore)
	mustDockerComposeKillHUP("nats-server")
	mustWaitForServer(st.ServiceA.User1Creds, 10*time.Second)
	containerIDAfter := mustDockerComposePSQ("nats-server")
	startedAfter := mustDockerInspectStartedAt(containerIDAfter)

	connB, err := connectWithCreds(credsPath(serviceBUser1), serviceBUser1)
	passD := err == nil
	actualD := "connect failed after reload"
	if passD {
		defer connB.Close()
		must(connB.Publish(serviceB.SubjectProbe, []byte("serviceb")), "serviceb publish")
		must(connB.FlushTimeout(2*time.Second), "serviceb flush")
		actualD = "connect ok after reload"
	}
	results = append(results, result{
		Name:       "Phase D new account after reload",
		Expected:   "connect after reload",
		Actual:     actualD,
		Pass:       passD,
		Conclusion: "새 account는 config reload 후 반영 가능해야 함",
	})

	statusBeforeFlush := connA1.Status().String()
	must(connA1.Publish(st.ServiceA.SubjectProbe, []byte("after-reload")), "servicea publish after reload")
	must(connA1.FlushTimeout(2*time.Second), "servicea flush after reload")
	statusAfterFlush := connA1.Status().String()
	passE := containerIDBefore == containerIDAfter && startedBefore == startedAfter
	actualE := fmt.Sprintf("container=%s startedAt(before=%s after=%s) connStatus=%s->%s", shortID(containerIDAfter), startedBefore, startedAfter, statusBeforeFlush, statusAfterFlush)
	results = append(results, result{
		Name:       "Phase E reload vs restart",
		Expected:   "startedAt unchanged + old conn usable",
		Actual:     actualE,
		Pass:       passE,
		Conclusion: "reload가 restart가 아니면 컨테이너 시작 시각이 유지되어야 함",
	})

	return results
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
	seed, err := operatorKP.Seed()
	if err != nil {
		return err
	}
	if err := mustWriteString(filepath.Join("generated", "auth", "operator", "operator.jwt"), operatorJWT); err != nil {
		return err
	}

	serviceA, err := createAccount(serviceAName, serviceAUser1, []string{"_INBOX.>", "servicea.>", "svc.echo"}, []string{"_INBOX.>", "servicea.>", "svc.echo"}, operatorKP)
	if err != nil {
		return err
	}
	if err := mustWriteString(credsPath(serviceAUser1), serviceA.UserCreds); err != nil {
		return err
	}
	state := state{
		OperatorSeed: string(seed),
		ServiceA: accountRecord{
			Name:         serviceA.Name,
			AccountSeed:  serviceA.AccountSeed,
			AccountPub:   serviceA.AccountPub,
			AccountJWT:   serviceA.AccountJWT,
			AllowedPub:   serviceA.AllowedPub,
			AllowedSub:   serviceA.AllowedSub,
			User1Creds:   credsPath(serviceAUser1),
			User1Name:    serviceA.UserName,
			SubjectProbe: serviceA.SubjectProbe,
		},
	}
	b, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	if err := mustWriteString(filepath.Join("generated", "state.json"), string(b)); err != nil {
		return err
	}
	return mustWriteConfig(state.ServiceA)
}

func createAccount(name, userName string, allowedPub, allowedSub []string, operatorKP nkeys.KeyPair) (generatedAccount, error) {
	accountKP, err := nkeys.CreateAccount()
	if err != nil {
		return generatedAccount{}, err
	}
	accountPub, err := accountKP.PublicKey()
	if err != nil {
		return generatedAccount{}, err
	}
	accountClaims := jwt.NewAccountClaims(accountPub)
	accountClaims.Name = name
	accountJWT, err := accountClaims.Encode(operatorKP)
	if err != nil {
		return generatedAccount{}, err
	}
	accountSeed, err := accountKP.Seed()
	if err != nil {
		return generatedAccount{}, err
	}
	userCreds, err := createUserCreds(string(accountSeed), userName, allowedPub, allowedSub)
	if err != nil {
		return generatedAccount{}, err
	}
	return generatedAccount{
		Name:         name,
		AccountSeed:  string(accountSeed),
		AccountPub:   accountPub,
		AccountJWT:   accountJWT,
		AllowedPub:   allowedPub,
		AllowedSub:   allowedSub,
		UserName:     userName,
		UserCreds:    userCreds,
		SubjectProbe: strings.ToLower(name) + ".runtime.check",
	}, nil
}

func mustCreateAccount(name, userName string, allowedPub, allowedSub []string) generatedAccount {
	st := mustLoadState()
	operatorKP := mustKeyPairFromSeed(st.OperatorSeed)
	acct, err := createAccount(name, userName, allowedPub, allowedSub, operatorKP)
	must(err, "create account "+name)
	return acct
}

func accountRecordFromGenerated(acct generatedAccount) accountRecord {
	return accountRecord{
		Name:         acct.Name,
		AccountSeed:  acct.AccountSeed,
		AccountPub:   acct.AccountPub,
		AccountJWT:   acct.AccountJWT,
		AllowedPub:   acct.AllowedPub,
		AllowedSub:   acct.AllowedSub,
		User1Creds:   credsPath(acct.UserName),
		User1Name:    acct.UserName,
		SubjectProbe: acct.SubjectProbe,
	}
}

func createUserCreds(accountSeed, userName string, allowedPub, allowedSub []string) (string, error) {
	accountKP, err := keyPairFromSeed(accountSeed)
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
	userClaims := jwt.NewUserClaims(userPub)
	userClaims.Name = userName
	for _, s := range allowedPub {
		userClaims.Permissions.Pub.Allow.Add(s)
	}
	for _, s := range allowedSub {
		userClaims.Permissions.Sub.Allow.Add(s)
	}
	userJWT, err := userClaims.Encode(accountKP)
	if err != nil {
		return "", err
	}
	userSeed, err := userKP.Seed()
	if err != nil {
		return "", err
	}
	creds, err := jwt.FormatUserConfig(userJWT, userSeed)
	if err != nil {
		return "", err
	}
	return string(creds), nil
}

func mustCreateUserCreds(accountSeed, userName string, allowedPub, allowedSub []string) string {
	creds, err := createUserCreds(accountSeed, userName, allowedPub, allowedSub)
	must(err, "create user creds "+userName)
	return creds
}

func mustLoadState() state {
	b, err := os.ReadFile(filepath.Join("generated", "state.json"))
	must(err, "read generated/state.json")
	var st state
	must(json.Unmarshal(b, &st), "parse state.json")
	return st
}

func mustWriteConfig(accounts ...accountRecord) error {
	entries := make([]string, 0, len(accounts))
	for _, acct := range accounts {
		entries = append(entries, fmt.Sprintf("  \"%s\": \"%s\"", acct.AccountPub, acct.AccountJWT))
	}
	cfg := fmt.Sprintf("port: 4222\nhttp_port: 8222\nserver_name: jwt-runtime-poc\noperator: \"/etc/nats/auth/operator/operator.jwt\"\nresolver: MEMORY\nresolver_preload: {\n%s\n}\n", strings.Join(entries, ",\n"))
	return mustWriteString("nats.conf", cfg)
}

func connectWithCreds(credsPath, label string) (*nats.Conn, error) {
	nc, err := nats.Connect(
		natsURL,
		nats.UserCredentials(credsPath),
		nats.Name(label),
		nats.Timeout(2*time.Second),
		nats.NoReconnect(),
	)
	if err != nil {
		return nil, err
	}
	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		nc.Close()
		return nil, err
	}
	return nc, nil
}

func mustWaitForServer(creds string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		nc, err := connectWithCreds(creds, "wait-for-server")
		if err == nil {
			nc.Close()
			return
		}
		last = err
		time.Sleep(300 * time.Millisecond)
	}
	must(last, "wait for server after reload")
}

func mustDockerComposePSQ(service string) string {
	out := mustCmd("docker", "compose", "ps", "-q", service)
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.Contains(line, "level=warning") {
			continue
		}
		return line
	}
	must(fmt.Errorf("empty container id from docker compose ps -q %s", service), "docker compose ps -q")
	return ""
}

func mustDockerInspectStartedAt(containerID string) string {
	out := mustCmd("docker", "inspect", "-f", "{{.State.StartedAt}}", containerID)
	return strings.TrimSpace(out)
}

func mustDockerComposeKillHUP(service string) {
	_ = mustCmd("docker", "compose", "kill", "-s", "HUP", service)
}

func mustCmd(name string, args ...string) string {
	cmd := exec.Command(name, args...)
	cmd.Dir = "."
	out, err := cmd.CombinedOutput()
	if err != nil {
		must(fmt.Errorf("%s %s failed: %w\n%s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out))), "command")
	}
	return string(out)
}

func mustWriteString(path, data string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(data), 0o644)
}

func credsPath(user string) string {
	return filepath.Join("generated", "creds", user+".creds")
}

func mustMkdir(path string) {
	must(os.MkdirAll(path, 0o755), "mkdir "+path)
}

func keyPairFromSeed(seed string) (nkeys.KeyPair, error) {
	return nkeys.FromSeed([]byte(seed))
}

func mustKeyPairFromSeed(seed string) nkeys.KeyPair {
	kp, err := keyPairFromSeed(seed)
	must(err, "load keypair from seed")
	return kp
}

func must(err error, msg string) {
	if err == nil {
		return
	}
	log.Fatalf("%s: %v", msg, err)
}

func shortID(s string) string {
	if len(s) <= 12 {
		return s
	}
	return s[:12]
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
