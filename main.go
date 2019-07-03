package main

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"time"

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
	"github.com/gorilla/schema"
	"github.com/pkg/errors"
)

type handler struct {
	db    *dynamodb.Client
	Table string
}

type History struct {
	ItemID  string
	Version int
	CurVer  int
	Who     string
	When    string
}

var itemID = "foo"
var views = template.Must(template.New("").ParseGlob("templates/*.html"))
var singapore, _ = time.LoadLocation("Asia/Singapore")

func New() (h handler, err error) {

	cfg, err := external.LoadDefaultAWSConfig(external.WithSharedConfigProfile("mine"))
	if err != nil {
		log.WithError(err).Fatal("setting up credentials")
		return
	}
	cfg.Region = endpoints.ApSoutheast1RegionID

	h = handler{db: dynamodb.New(cfg), Table: "History2"}
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
	app.HandleFunc("/v/{id}", h.lookup)
	app.HandleFunc("/", h.redirectToLatest)
	app.HandleFunc("/add", h.add)
	app.HandleFunc("/all", h.all)
	if err := http.ListenAndServe(addr, app); err != nil {
		log.WithError(err).Fatal("error listening")
	}
}

func add(w http.ResponseWriter, r *http.Request) {
	views.ExecuteTemplate(w, "index.html", nil)
}

// Advanced design patterns suggests a transaction
// https://s.natalian.org/2019-07-03/amazon-dynamodb-deep-dive-advanced-design-patterns-for-dynamodb-dat401-aws-reinvent-2018pdf-41-638.jpg
func (h handler) add(w http.ResponseWriter, r *http.Request) {
	var decoder = schema.NewDecoder()
	err := r.ParseForm()
	if err != nil {
		log.WithError(err).Error("failed to parse form")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var add History
	err = decoder.Decode(&add, r.PostForm)
	if err != nil {
		log.WithError(err).Error("failed to decode")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	latestVersion, err := h.latest()
	if err != nil {
		log.WithError(err).Error("failed to query table")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Infof("%#v", add)
	add.ItemID = itemID
	add.Version = latestVersion.CurVer + 1
	add.When = time.Now().In(singapore).Format(time.RFC3339)

	log.Infof("saving %v", add)
	av, err := dynamodbattribute.MarshalMap(add)
	if err != nil {
		log.WithError(err).Error("failed to marshal selection")
		return
	}

	req := h.db.PutItemRequest(&dynamodb.PutItemInput{
		TableName: aws.String(h.Table),
		Item:      av,
	})
	_, err = req.Send(context.Background())
	if err != nil {
		log.WithField("table", h.Table).WithError(err).Error("putting dynamodb")
		return
	}

	// https://godoc.org/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.UpdateItemRequest

	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames: map[string]string{
			"#Y": "CurVer",
		},
		ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
			":y": {
				N: aws.String(fmt.Sprintf("%d", add.Version)),
			},
		},
		Key: map[string]dynamodb.AttributeValue{
			"ItemID": {
				S: aws.String(itemID),
			},
			"Version": {
				N: aws.String("0"),
			},
		},
		TableName:        aws.String(h.Table),
		UpdateExpression: aws.String("SET #Y = :y"),
	}

	updateReq := h.db.UpdateItemRequest(input)

	_, err = updateReq.Send(context.Background())
	if err != nil {
		log.WithField("table", h.Table).WithError(err).Error("updating dynamodb")
		return
	}

	http.Redirect(w, r, "/", http.StatusFound)
}

func (h handler) latest() (first History, err error) {
	iReq := h.db.GetItemRequest(&dynamodb.GetItemInput{
		Key: map[string]dynamodb.AttributeValue{
			"ItemID": {
				S: aws.String(itemID),
			},
			"Version": {
				N: aws.String("0"),
			},
		},
		ProjectionExpression: aws.String("CurVer"),
		TableName:            aws.String(h.Table),
	})
	result, err := iReq.Send(context.Background())
	if err != nil {
		return first, errors.Wrap(err, "send")
	}
	err = dynamodbattribute.UnmarshalMap(result.Item, &first)
	return first, errors.Wrap(err, "unmarshal")
}

func (h handler) redirectToLatest(w http.ResponseWriter, r *http.Request) {
	latestVersion, err := h.latest()
	if err != nil {
		log.WithError(err).Error("failed to query table")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Infof("Current version: %+v", latestVersion)
	http.Redirect(w, r, fmt.Sprintf("/v/%d", latestVersion.CurVer), http.StatusFound)
}

func (h handler) lookup(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	iReq := h.db.GetItemRequest(&dynamodb.GetItemInput{
		Key: map[string]dynamodb.AttributeValue{
			"ItemID": {
				S: aws.String(itemID),
			},
			"Version": {
				N: aws.String(vars["id"]),
			},
		},
		TableName: aws.String(h.Table),
	})
	result, err := iReq.Send(context.Background())
	if err != nil {
		log.WithError(err).Errorf("failed to query table: %v", vars)
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
	log.Infof("Version: %s %v", vars["id"], current)

	err = views.ExecuteTemplate(w, "show.html", struct {
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
	keyCond := expression.Key("ItemID").Equal(expression.Value(itemID))

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
