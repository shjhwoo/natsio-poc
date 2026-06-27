package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

type state struct {
	ServiceA struct {
		AccountSeed string `json:"account_seed"`
	} `json:"service_a"`
}

func main() {
	b, err := os.ReadFile(filepath.Join("generated", "state.json"))
	if err != nil { panic(err) }
	var st state
	if err := json.Unmarshal(b, &st); err != nil { panic(err) }
	accountKP, err := nkeys.FromSeed([]byte(st.ServiceA.AccountSeed))
	if err != nil { panic(err) }
	userKP, err := nkeys.CreateUser()
	if err != nil { panic(err) }
	userPub, err := userKP.PublicKey()
	if err != nil { panic(err) }
	uc := jwt.NewUserClaims(userPub)
	uc.Name = "servicea-user-2-manual"
	uc.Permissions.Pub.Allow.Add("svc.echo")
	uc.Permissions.Sub.Allow.Add("svc.echo")
	userJWT, err := uc.Encode(accountKP)
	if err != nil { panic(err) }
	seed, err := userKP.Seed()
	if err != nil { panic(err) }
	creds, err := jwt.FormatUserConfig(userJWT, seed)
	if err != nil { panic(err) }
	credsPath := filepath.Join("evidence", "servicea-user-2-manual.creds")
	if err := os.WriteFile(credsPath, creds, 0o644); err != nil { panic(err) }
	nc, err := nats.Connect("nats://127.0.0.1:4222", nats.UserCredentials(credsPath), nats.Timeout(2*time.Second), nats.NoReconnect())
	if err != nil { panic(err) }
	defer nc.Close()
	if err := nc.Publish("svc.echo", []byte("ok")); err != nil { panic(err) }
	if err := nc.FlushTimeout(2 * time.Second); err != nil { panic(err) }
	fmt.Println("manual existing-account new-user connect+publish ok")
}
