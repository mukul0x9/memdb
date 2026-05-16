package main

func main() {
	var db DB

	db.Init()

	tcpListener(&db)
}
