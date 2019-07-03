package main

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"os"

	"github.com/apex/log"
	jsonhandler "github.com/apex/log/handlers/json"
	"github.com/apex/log/handlers/text"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/endpoints"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/expression"
	"github.com/gorilla/mux"
)

type handler struct {
	db    *dynamodb.Client
	Table string
}

type History struct {
	ItemID  string
	Version string
	CurVer  string
	Who     string
	When    string
}

var views = template.Must(template.New("").ParseGlob("templates/*.html"))

func New() (h handler, err error) {

	cfg, err := external.LoadDefaultAWSConfig(external.WithSharedConfigProfile("mine"))
	if err != nil {
		log.WithError(err).Fatal("setting up credentials")
		return
	}
	cfg.Region = endpoints.ApSoutheast1RegionID

	h = handler{db: dynamodb.New(cfg), Table: "History"}
	return h, err

}

func main() {

	if os.Getenv("UP_STAGE") == "" {
		log.SetHandler(text.Default)
	} else {
		log.SetHandler(jsonhandler.Default)
	}

	h, err := New()
	if err != nil {
		log.WithError(err).Fatal("error setting configuration")
		return
	}

	addr := ":" + os.Getenv("PORT")
	app := mux.NewRouter()
	app.HandleFunc("/latest", h.latest)
	app.HandleFunc("/", h.all)
	if err := http.ListenAndServe(addr, app); err != nil {
		log.WithError(err).Fatal("error listening")
	}
}

func add(w http.ResponseWriter, r *http.Request) {
	views.ExecuteTemplate(w, "index.html", nil)
}

func (h handler) latest(w http.ResponseWriter, r *http.Request) {
	iReq := h.db.GetItemRequest(&dynamodb.GetItemInput{
		Key: map[string]dynamodb.AttributeValue{
			"ItemID": {
				S: aws.String("1"),
			},
			"Version": {
				S: aws.String("v0"),
			},
		},
		ProjectionExpression: aws.String("CurVer"),
		TableName:            aws.String("History"),
	})
	result, err := iReq.Send(context.Background())
	if err != nil {
		log.WithError(err).Error("failed to query table")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var first History
	err = dynamodbattribute.UnmarshalMap(result.Item, &first)
	if err != nil {
		log.WithError(err).Error("unmarshal dynamodb result")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	log.Infof("Current version: %+v", first.CurVer)
	iReq = h.db.GetItemRequest(&dynamodb.GetItemInput{
		Key: map[string]dynamodb.AttributeValue{
			"ItemID": {
				S: aws.String("1"),
			},
			"Version": {
				S: aws.String(first.CurVer),
			},
		},
		TableName: aws.String("History"),
	})
	result, err = iReq.Send(context.Background())
	if err != nil {
		log.WithError(err).Error("failed to query table")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var current History
	err = dynamodbattribute.UnmarshalMap(result.Item, &current)
	if err != nil {
		log.WithError(err).Error("unmarshal dynamodb result")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	log.Infof("Version: %s %v", first.CurVer, current)

	err = views.ExecuteTemplate(w, "latest.html", struct {
		History History
	}{
		current,
	})

	if err != nil {
		log.WithError(err).Error("failed to render HTML")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

}

func (h handler) all(w http.ResponseWriter, r *http.Request) {
	keyCond := expression.Key("ItemID").Equal(expression.Value("1"))

	expr, err := expression.NewBuilder().
		WithKeyCondition(keyCond).
		Build()
	if err != nil {
		fmt.Println(err)
	}

	// Make the DynamoDB Query API call
	scanReq := h.db.QueryRequest(&dynamodb.QueryInput{
		ExpressionAttributeValues: expr.Values(),
		ExpressionAttributeNames:  expr.Names(),
		KeyConditionExpression:    expr.KeyCondition(),
		TableName:                 aws.String(h.Table),
	})
	result, err := scanReq.Send(context.Background())
	if err != nil {
		log.WithError(err).Error("failed to scan table")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	items := []History{}
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &items)
	if err != nil {
		log.WithError(err).Error("unmarshal dynamodb results")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = views.ExecuteTemplate(w, "list.html", struct {
		History []History
	}{
		items,
	})

	if err != nil {
		log.WithError(err).Error("failed to render HTML")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

}
