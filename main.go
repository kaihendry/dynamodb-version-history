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
	app.HandleFunc("/", h.current)
	if err := http.ListenAndServe(addr, app); err != nil {
		log.WithError(err).Fatal("error listening")
	}
}

func add(w http.ResponseWriter, r *http.Request) {
	views.ExecuteTemplate(w, "index.html", nil)
}

func parseDate(input string, tz string) (day time.Time, err error) {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		log.WithError(err).Error("bad timezone")
		return
	}

	day, err = time.ParseInLocation("2006-01-02", input, loc)
	if err != nil {
		log.WithError(err).Info("bad date")
		return
	}

	return
}

func (h handler) current(w http.ResponseWriter, r *http.Request) {
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
