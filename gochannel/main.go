package main

// import (
// 	"encoding/json"
// 	"fmt"
// 	"net/http"
// 	"time"
// )

// var count int

// type Response struct {
// 	Values []int `json:"values"`
// }

// // ---- Dummy APIs ----

// func apiOne(w http.ResponseWriter, r *http.Request) {
// 	json.NewEncoder(w).Encode(Response{Values: []int{1, 2, 3, 4, 5}})
// }

// func apiTwo(w http.ResponseWriter, r *http.Request) {
// 	json.NewEncoder(w).Encode(Response{Values: []int{6, 7, 8, 9, 10}})
// }

// // ---- Worker ----

// func fetchAndProcess(url string, out chan<- []int) {
// 	resp, err := http.Get(url)
// 	if err != nil {
// 		out <- nil
// 		return
// 	}
// 	defer resp.Body.Close()

// 	var r Response
// 	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
// 		out <- nil
// 		return
// 	}

// 	result := make([]int, len(r.Values))
// 	for i, v := range r.Values {
// 		time.Sleep(100 * time.Millisecond)
// 		result[i] = v * v
// 	}

// 	out <- result
// }

// // ---- Combine ----

// func combine(a, b []int) []int {
// 	return append(a, b...)
// }

// // ---- Main Handler ----

// func handler(w http.ResponseWriter, r *http.Request) {
// 	count++
// 	fmt.Println("Handling request:", count)

// 	ch1 := make(chan []int)
// 	ch2 := make(chan []int)

// 	go fetchAndProcess("http://localhost:8080/api/one", ch1)
// 	go fetchAndProcess("http://localhost:8080/api/two", ch2)
// select{
// 	res1 := <-ch1
	
// 	res2 := <-ch2
// }

// 	final := combine(res1, res2)
// 	json.NewEncoder(w).Encode(final)
// }

// // ---- main ----

// func main() {
// 	http.HandleFunc("/api/one", apiOne)
// 	http.HandleFunc("/api/two", apiTwo)
// 	http.HandleFunc("/data", handler)

// 	fmt.Println("server running on :8080")
// 	http.ListenAndServe(":8080", nil)
// }
// func lengthOfLongestSubstring(s string) int {

//     if len(s) <=1 {
//         if len(s) == 0 {
//             return 0
//         }
//         return 1
    
//     }

//     i := 0 
//     j := 1
//     maxStr := 0
//     curStr := 0

//     Hmap := make(map[rune]int)

//     for j < len(s){

//         if _ , ok := Hmap[rune(s[j])] ; ok  {

//                 Hmap[rune(s[j])] = 1
//                 i = j 
//                 j++

//                 if curStr > maxStr {
//                     maxStr = curStr
//                     curStr = 0 
//                 }
//                 continue
//         }

//         Hmap[rune(s[j])] = 1
//         j++
//     }
//     return maxStr

// }



import (
	"fmt"
)

func lengthOfLongestSubstring(s string) int {
	if len(s) <= 1 {
		if len(s) == 0 {
			return 0
		}
		return 1
	}

	i := 0
	j := 0
	maxStr := 0

	// use byte map because we index by s[j] (byte-wise)
	Hmap := make(map[byte]int)

	for j < len(s) {
		// if current char is already in window (count == 1), shrink from left
		if Hmap[s[j]] == 1 {
			// update max with current window length
			if j-i > maxStr {
				maxStr = j - i
			}
			// move i until the duplicate character s[j] is removed from the window
			for Hmap[s[j]] == 1 && i < j {
				Hmap[s[i]]--
				i++
			}
			// now Hmap[s[j]] == 0, we'll add s[j] below
		}

		// add current character and expand window
		Hmap[s[j]]++
		j++
	}

	// final window check
	if j-i > maxStr {
		maxStr = j - i
	}

	return maxStr
}

func main() {
	tests := []string{
		"pwwkew",
		"bbbbb",
		"abcabcbb",
	}

	for _, t := range tests {
		fmt.Printf("%q -> %d\n", t, lengthOfLongestSubstring(t))
	}
}
