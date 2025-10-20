package main

import (
	"fmt"
	"log"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

const natsURL = nats.DefaultURL

const STARFRUIT = "STARFRUIT"
const VEGAS = "VEGAS"
const VEGAS_EVENT = "VEGAS_EVENT"
const IMS = "IMS"

type PubSubAllowAndJS struct {
	Pub              []string
	Sub              []string
	JetstreamEnabled bool
}

var userPubSubAllowMap = map[string]PubSubAllowAndJS{
	STARFRUIT: {
		Pub:              []string{"_INBOX.>", "starfruit.internal.>", "svc.echo"},
		Sub:              []string{"_INBOX.>", "starfruit.internal.>", "svc.echo"},
		JetstreamEnabled: true,
	},
	VEGAS: {
		Pub:              []string{"_INBOX.>", "vegas.starfruit.>", "svc.echo"},
		Sub:              []string{"_INBOX.>", "vegas.starfruit.>", "svc.echo"},
		JetstreamEnabled: false,
	},
	VEGAS_EVENT: {
		Sub:              []string{"internal.vegas.pub.>", "svc.echo"},
		JetstreamEnabled: false,
	},
	IMS: {
		Pub:              []string{"_INBOX.>", "emr.ims.req.>", "svc.echo"},
		Sub:              []string{"_INBOX.>", "svc.echo"},
		JetstreamEnabled: false,
	},
}

func main() {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		panic(err)
	}
	defer nc.Close()

	operatorKP, _ := nkeys.CreateOperator()
	log.Println("NATS Operator 키 생성 성공")

	operatorPub, _ := operatorKP.PublicKey()
	fmt.Printf("operator pubkey: %s\n", operatorPub)

	operatorSeed, _ := operatorKP.Seed()
	fmt.Printf("operator seed: %s\n\n", string(operatorSeed))

	for name, pubsubAndJs := range userPubSubAllowMap {
		accountKP, _ := nkeys.CreateAccount()

		accountPub, _ := accountKP.PublicKey()
		fmt.Printf("account pubkey: %s\n", accountPub)

		accountSeed, _ := accountKP.Seed()
		fmt.Printf("account seed: %s\n", string(accountSeed))

		accountClaims := jwt.NewAccountClaims(accountPub)
		accountClaims.Name = name

		if pubsubAndJs.JetstreamEnabled {
			accountClaims.Limits.JetStreamLimits = jwt.JetStreamLimits{
				MemoryStorage: -1,
				DiskStorage:   -1,
			}
		}

		fmt.Printf("account claims: %s\n", accountClaims)
		accountJWT, _ := accountClaims.Encode(operatorKP)
		fmt.Printf("starfruit account jwt: %s\n\n", accountJWT)

		userKP, _ := nkeys.CreateUser()

		userPub, _ := userKP.PublicKey()
		fmt.Printf("user pubkey: %s\n", userPub)

		userSeed, _ := userKP.Seed()
		fmt.Printf("user seed: %s\n", string(userSeed))

		userClaims := jwt.NewUserClaims(userPub)
		userClaims.Name = name

		userClaims.Limits.Data = 1024 * 1024 * 1024

		for _, pubSubject := range pubsubAndJs.Pub {
			userClaims.Permissions.Pub.Allow.Add(pubSubject)
		}

		for _, subSubject := range pubsubAndJs.Sub {
			userClaims.Permissions.Sub.Allow.Add(subSubject)
		}

		fmt.Printf("userclaims: %s\n", userClaims)

		userJWT, _ := userClaims.Encode(accountKP)
		fmt.Printf("user jwt: %s\n", userJWT)

		creds, _ := jwt.FormatUserConfig(userJWT, userSeed)
		fmt.Printf("creds file: %s\n", creds)
	}
}
