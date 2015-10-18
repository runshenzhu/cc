package main

import (
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
	team_info = "Omegaga's Black Railgun,6537-0651-1730"
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
	return string(z)
}

func q1Handler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	message := r.URL.Query().Get("message")
	keyInt := new(big.Int)
	keyInt.SetString(key, 10)
	y := new(big.Int)
	y.Div(keyInt, X)
	y_mod := int(y.Mod(y, big.NewInt(25)).Int64())
	z := 1 + y_mod
	decode_str := decode(message, z)

	// construct response string
	utc_t := time.Now().UTC()
	t := utc_t.Add(-4 * time.Hour)
	output_str := team_info + "\n" +
		t.Format("2006-01-02 15:04:05") + "\n" +
		decode_str + "\n"
	w.Write([]byte(output_str))
}

func heartbeatHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("You are good\n"))
}

func main() {
	X = new(big.Int)
	runtime.GOMAXPROCS(runtime.NumCPU())
	X.SetString("8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773", 10)
	http.HandleFunc("/q1", q1Handler)
	http.HandleFunc("/heartbeat", heartbeatHandler)
	http.ListenAndServe(":80", nil)
}
