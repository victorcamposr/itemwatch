// Command ledgersim writes synthetic traffic into a ledger so itemwatch can
// be tried without a game server: steady loot for a few players, and from
// time to time one of the anomalies the rules look for.
package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"flag"
	"log"
	mrand "math/rand/v2"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var monsters = []string{"loot:Dragon", "loot:Hydra", "loot:Giant Spider", "loot:Rotworm"}

func main() {
	dsn := flag.String("dsn", "root:itemwatch@tcp(localhost:3306)/game", "game database DSN")
	rate := flag.Int("rate", 5, "normal births per second")
	every := flag.Duration("anomaly-every", 3*time.Minute, "interval between injected anomalies")
	flag.Parse()

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; ; i++ {
		if err = db.Ping(); err == nil {
			break
		}
		if i == 30 {
			log.Fatal(err)
		}
		time.Sleep(2 * time.Second)
	}
	mustExec(db, "INSERT IGNORE INTO players (id, name) VALUES (1,'Aria'),(2,'Bram'),(3,'Cora'),(4,'Dain'),(5,'Esme')")

	tick := time.NewTicker(time.Second)
	anomaly := time.NewTicker(*every)
	scenario := 0
	log.Printf("writing %d births/s, an anomaly every %s", *rate, *every)
	for {
		select {
		case <-tick.C:
			for range *rate {
				birth(db, monsters[mrand.IntN(len(monsters))], int64(1+mrand.IntN(5)), "")
			}
		case <-anomaly.C:
			inject(db, scenario%4)
			scenario++
		}
	}
}

func inject(db *sql.DB, scenario int) {
	switch scenario {
	case 0:
		log.Print("anomaly: quest reward paid four times")
		for range 4 {
			birth(db, "quest:Annihilator", 2, "")
		}
	case 1:
		h := hash()
		log.Printf("anomaly: item %s duplicated into another depot", h)
		mustExec(db, "INSERT INTO player_items (player_id, sid, itemtype, hash) VALUES (3, 1, 3366, ?)", h)
		mustExec(db, "INSERT INTO player_depotitems (player_id, sid, itemtype, hash) VALUES (4, 1, 3366, ?)", h)
	case 2:
		log.Print("anomaly: item saved without a hash")
		mustExec(db, "INSERT INTO item_hash_log (hash, event, itemtype, origin, source, created_at) VALUES ('', 'unhashed', 3079, 'script', 'scripts/actions/broken_chest.lua', ?)", time.Now().Unix())
	case 3:
		log.Print("anomaly: 1000 extra Rotworm drops within a minute, spread over every player")
		for range 1000 {
			birth(db, "loot:Rotworm", int64(1+mrand.IntN(5)), "")
		}
	}
}

func birth(db *sql.DB, origin string, player int64, source string) {
	mustExec(db, "INSERT INTO item_hash_log (hash, event, itemtype, count, origin, source, player_id, created_at) VALUES (?, 'birth', ?, 1, ?, ?, ?, ?)",
		hash(), 3000+mrand.IntN(500), origin, source, player, time.Now().Unix())
}

func hash() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func mustExec(db *sql.DB, q string, args ...any) {
	if _, err := db.Exec(q, args...); err != nil {
		log.Fatal(err)
	}
}
