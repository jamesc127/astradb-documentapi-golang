package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
)

type Astra struct {
	DbId       string
	Region     string
	Keyspace   string
	Collection string
	Token      string
}

type Person struct {
	Address struct {
		City   string `json:"city"`
		State  string `json:"state"`
		Street string `json:"street"`
	} `json:"address"`
	Email    string `json:"email"`
	Favorite struct {
		Animal []string `json:"animal"`
		Color  string   `json:"color"`
	} `json:"favorite"`
	FirstName string `json:"first_name"`
	Friends   struct {
		Name []string `json:"name"`
	} `json:"friends"`
	Gender    string `json:"gender"`
	ID        int    `json:"id"`
	IPAddress string `json:"ip_address"`
	LastName  string `json:"last_name"`
}

type CarJson struct {
	Car struct {
		Vin   string `json:"VIN"`
		Color string `json:"color"`
		Make  string `json:"make"`
		Model string `json:"model"`
		Year  int    `json:"year"`
	} `json:"car"`
	ID int `json:"id"`
}

//https://github.com/cameronldroberts/golang-api/blob/master/main.go
//https://levelup.gitconnected.com/consuming-a-rest-api-using-golang-b323602ba9d8

func PrettyString(str []byte) (string, error) {
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, str, "", "  "); err != nil {
		return "", err
	}
	return prettyJSON.String(), nil
}

func main() {
	AstraDB := Astra{
		DbId:       "ef575c27-6064-40a1-9336-37af9f7a983f",
		Region:     "us-central1",
		Keyspace:   "test_keyspace",
		Collection: "one",
		Token:      "AstraCS:EyINoFwdCWbLqzCUEPJdtWDf:f7161e51e377159d0ae6a5370caec8ac661bdfcad0cf23e82e63968d552a515b",
	}
	client := &http.Client{}

	//People Stuff
	personJson, err := os.Open("MOCK_DATA.json")
	if err != nil {
		fmt.Println(err)
	}
	personByteValue, _ := io.ReadAll(personJson)
	var people []Person
	json.Unmarshal(personByteValue, &people)
	for i := 0; i < len(people); i++ {
		person, _ := json.Marshal(people[i])
		req, err := http.NewRequest("POST",
			"https://"+AstraDB.DbId+"-"+AstraDB.Region+".apps.astra.datastax.com/api/rest/v2/namespaces/"+
				AstraDB.Keyspace+"/collections/"+AstraDB.Collection, bytes.NewBuffer(person))
		if err != nil {
			fmt.Print(err.Error())
		}
		req.Header.Add("X-Cassandra-Token", AstraDB.Token)
		req.Header.Add("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			fmt.Print(err.Error())
		}
		body, _ := io.ReadAll(resp.Body)
		fmt.Println("Person " + string(body))
		err = resp.Body.Close()
		if err != nil {
			fmt.Print(err.Error())
		}
	}

	//Car Stuff
	carJson, carErr := os.Open("MOCK_DATA_CAR.json")
	if carErr != nil {
		fmt.Println(carErr)
	}
	carByteValue, _ := io.ReadAll(carJson)
	var allCars []CarJson
	json.Unmarshal(carByteValue, &allCars)
	for i := 0; i < len(allCars); i++ {
		onecar, _ := json.Marshal(allCars[i])
		req, err := http.NewRequest("POST",
			"https://"+AstraDB.DbId+"-"+AstraDB.Region+".apps.astra.datastax.com/api/rest/v2/namespaces/"+
				AstraDB.Keyspace+"/collections/"+AstraDB.Collection, bytes.NewBuffer(onecar))
		if err != nil {
			fmt.Print(err.Error())
		}
		req.Header.Add("X-Cassandra-Token", AstraDB.Token)
		req.Header.Add("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			fmt.Print(err.Error())
		}
		body, _ := io.ReadAll(resp.Body)
		fmt.Println("Car " + string(body))
		err = resp.Body.Close()
		if err != nil {
			fmt.Print(err.Error())
		}
	}

	//Query Stuff
	where := url.QueryEscape("{\"address.state\":{\"$eq\":\"Texas\"}}")
	query, queryErr := http.NewRequest("GET", "https://"+AstraDB.DbId+"-"+AstraDB.Region+".apps.astra.datastax.com/api/rest/v2/namespaces/"+
		AstraDB.Keyspace+"/collections/"+AstraDB.Collection+"?where="+where, nil)
	if queryErr != nil {
		fmt.Println(queryErr)
	}
	query.Header.Add("X-Cassandra-Token", AstraDB.Token)
	queryRes, queryErr := client.Do(query)
	if queryErr != nil {
		fmt.Println(queryErr)
		return
	}
	defer queryRes.Body.Close()
	body, bodyErr := io.ReadAll(queryRes.Body)
	if bodyErr != nil {
		fmt.Println(bodyErr)
		return
	}
	fmt.Println(PrettyString(body))

}
