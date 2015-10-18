package main

import (
	"fmt"
	"math"
	"math/big"
	"net/http"
	"runtime"
	"time"
)

var X = new(big.Int)

const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

// basic config
const (
	team_id         = "Omegaga's Black Railgun"
	team_account_id = "6537-0651-1730"
)

func decode(a string, shift int) string {
	n := len(a)
	z := make([]byte, n)
	x := 0
	y := 0
	k := int(math.Sqrt(float64(n)))
	next := 0
	for i := 0; i < n; i++ {
		j := x*k + y
		z[i] = alphabet[(int(a[j])-int('A')+26-shift)%26]
		if x == k-1 {
			y = k - 1
			x = k - next
			next--
		} else if y == 0 {
			x = 0
			next++
			y = next
		} else {
			x++
			y--
		}
	}
	return string(z[:n])
}

func handler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	message := r.URL.Query().Get("message")
	keyInt := new(big.Int)
	keyInt.SetString(key, 10)
	y := new(big.Int)
	y.Div(keyInt, X)
	z := new(big.Int)
	z.Add(z.Mod(y, big.NewInt(25)), big.NewInt(1))
	decode_str := decode(message, int(z.Int64()))

	// construct response string
	utc_t := time.Now().UTC()
	t := utc_t.Add(-14400 * time.Second)
	fmt.Fprintf(w, "%s,%s\n%s\n%s\n",
		team_id, team_account_id,
		t.Format("2006-01-02 15:04:05"),
		decode_str)
}

func main() {
	X = new(big.Int)
	runtime.GOMAXPROCS(runtime.NumCPU())
	X.SetString("8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773", 10)
	http.HandleFunc("/q1", handler)
	http.ListenAndServe(":80", nil)
}
