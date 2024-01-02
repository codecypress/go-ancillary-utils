package main

import (
	"fmt"
	"github.com/codecypress/go-ancillary-utils/cypressutils"
)

func main() {
	conDSN := &cypressutils.DBConDSN{
		OrganizationId:       "-1L",
		DatabaseServer:       cypressutils.DbTypePostgresql,
		DatabaseName:         "sky_bc365proxy_db",
		DatabaseHost:         "192.168.90.75",
		UserName:             "root",
		Password:             "toor96",
		ConnectionParameters: "",
		Port:                 5432,
		MaxIdleConnections:   10,
		MaxOpenConnections:   40,
	}

	err := cypressutils.SetupDSN("-1L", conDSN)
	if err != nil {
		cypressutils.ThrowException(err)
		return
	} else {
		fmt.Println(" Successfully Connected to Database...")
	}

}
