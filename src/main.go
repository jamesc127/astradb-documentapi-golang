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

func PrettyString(str []byte) (string, error) {
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, str, "", "  "); err != nil {
		return "", err
	}
	return prettyJSON.String(), nil
}

func AstraQuery(client http.Client, AstraDB Astra, where string) (string, error) {
	query, err := http.NewRequest("GET", "https://"+AstraDB.DbId+"-"+AstraDB.Region+
		".apps.astra.datastax.com/api/rest/v2/namespaces/"+
		AstraDB.Keyspace+"/collections/"+AstraDB.Collection+
		"?where="+url.QueryEscape(where), nil)
	query.Header.Add("X-Cassandra-Token", AstraDB.Token)
	queryRes, err := client.Do(query)
	body, err := io.ReadAll(queryRes.Body)
	response, err := PrettyString(body)
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			return
		}
	}(queryRes.Body)
	if err != nil {
		return "", err
	}
	return response, nil
}

func main() {
	AstraDB := Astra{
		DbId:       "c8a1507b-31d9-4044-908b-01dc0482694c",
		Region:     "us-central1",
		Keyspace:   "test_ks",
		Collection: "one",
		Token:      "AstraCS:IhAZGCrnSOTRjMssZFwcLRYc:d73d62ee035c9cd452653283e6ec524fdc90b8eb8e248c26cefa086cb6bdcb75",
	}
	client := http.Client{}

	//Open the People dataset file
	personJson, err := os.Open("MOCK_DATA.json")
	if err != nil {
		fmt.Println(err.Error())
	}
	personByteValue, _ := io.ReadAll(personJson)
	var people []Person

	//Unmarshal the dataset into JSON
	err = json.Unmarshal(personByteValue, &people)
	if err != nil {
		fmt.Println(err)
	}

	//Load each Person into Astra DB
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
		//Print the Person document ID
		fmt.Println("Person " + string(body))
		err = resp.Body.Close()
		if err != nil {
			fmt.Print(err.Error())
		}
	}

	//Open the Car dataset file
	carJson, carErr := os.Open("MOCK_DATA_CAR.json")
	if carErr != nil {
		fmt.Println(carErr)
	}
	carByteValue, _ := io.ReadAll(carJson)
	var allCars []CarJson

	//Unmarshal the dataset into JSON
	err = json.Unmarshal(carByteValue, &allCars)
	if err != nil {
		fmt.Println(err)
	}

	//Load each Car into Astra DB
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

		//Print the Car document ID
		fmt.Println("Car " + string(body))
		err = resp.Body.Close()
		if err != nil {
			fmt.Print(err.Error())
		}
	}

	//Where clause that finds all People that live in Texas
	where := "{\"address.state\":{\"$eq\":\"Texas\"}}"
	body, err := AstraQuery(client, AstraDB, where)
	fmt.Println("---- HERE'S THE STATE QUERY ----")
	fmt.Println(body)

	//Where clause that finds all Cars with a year model greater than 2005
	where = "{\"car.year\":{\"$gt\":2005}}"
	body, err = AstraQuery(client, AstraDB, where)
	fmt.Println("---- HERE'S THE CAR YEAR QUERY ----")
	fmt.Println(body)

	//Where clause that finds ANY attribute that equals Blue
	where = "{\"*.color\":{\"$eq\":\"Blue\"}}"
	body, err = AstraQuery(client, AstraDB, where)
	fmt.Println("---- HERE'S THE COLOR QUERY ----")
	fmt.Println(body)

}
