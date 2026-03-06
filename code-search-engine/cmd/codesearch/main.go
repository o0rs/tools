package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
)

type Posting struct {
	DocID int `json:"d"`
	TF    int `json:"t"`
}

type Document struct {
	Path   string `json:"p"`
	Length int    `json:"l"`
}

type Index struct {
	Version   int                  `json:"version"`
	Root      string               `json:"root"`
	CreatedAt string               `json:"created_at"`
	Docs      []Document           `json:"docs"`
	Inverted  map[string][]Posting `json:"inverted"`
	TotalDocs int                  `json:"total_docs"`
	AvgDocLen float64              `json:"avg_doc_len"`
}

type BuildStats struct {
	ScannedFiles int
	IndexedFiles int
	SkippedFiles int
}

var defaultIgnoredDirs = map[string]struct{}{
	".git":         {},
	"node_modules": {},
	"vendor":       {},
	"target":       {},
	"dist":         {},
	"build":        {},
	".idea":        {},
	".vscode":      {},
	".venv":        {},
	"venv":         {},
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "index":
		if err := runIndex(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "index error: %v\n", err)
			os.Exit(1)
		}
	case "query":
		if err := runQuery(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "query error: %v\n", err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Println("Local Code Search Engine")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  go build -o ./bin/codesearch ./cmd/codesearch")
	fmt.Println("  ./bin/codesearch index -root . -out index.json -workers 8 -max-file 1048576")
	fmt.Println("  ./bin/codesearch query -index index.json -q 'goroutine channel' -top 10")
}

func runIndex(args []string) error {
	fs := flag.NewFlagSet("index", flag.ContinueOnError)
	root := fs.String("root", ".", "root directory to scan")
	out := fs.String("out", "index.json", "output index file")
	workers := fs.Int("workers", runtime.NumCPU(), "number of worker goroutines")
	maxFileSize := fs.Int64("max-file", 1<<20, "maximum file size in bytes")
	extFilter := fs.String("ext", "", "optional comma-separated extensions, e.g. .go,.rs,.md")
	if err := fs.Parse(args); err != nil {
		return err
	}

	absRoot, err := filepath.Abs(*root)
	if err != nil {
		return err
	}

	allowedExt := parseExtFilter(*extFilter)
	idx, stats, err := buildIndex(absRoot, *workers, *maxFileSize, allowedExt)
	if err != nil {
		return err
	}
	if err := saveIndex(*out, idx); err != nil {
		return err
	}

	fmt.Printf("indexed files: %d/%d (skipped: %d), terms: %d, docs: %d, avgdl: %.2f\n",
		stats.IndexedFiles, stats.ScannedFiles, stats.SkippedFiles, len(idx.Inverted), idx.TotalDocs, idx.AvgDocLen)
	fmt.Printf("index written to %s\n", *out)
	return nil
}

func runQuery(args []string) error {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	indexPath := fs.String("index", "index.json", "path to index JSON")
	q := fs.String("q", "", "query string")
	topK := fs.Int("top", 10, "top K results")
	k1 := fs.Float64("k1", 1.2, "BM25 k1")
	b := fs.Float64("b", 0.75, "BM25 b")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*q) == "" {
		return errors.New("query is empty, use -q")
	}

	idx, err := loadIndex(*indexPath)
	if err != nil {
		return err
	}
	results := bm25Query(idx, *q, *k1, *b, *topK)
	if len(results) == 0 {
		fmt.Println("no match")
		return nil
	}

	for i, r := range results {
		fmt.Printf("%2d. %.4f  %s\n", i+1, r.Score, idx.Docs[r.DocID].Path)
	}
	return nil
}

func parseExtFilter(raw string) map[string]struct{} {
	out := map[string]struct{}{}
	if strings.TrimSpace(raw) == "" {
		return out
	}
	for _, s := range strings.Split(raw, ",") {
		s = strings.TrimSpace(strings.ToLower(s))
		if s == "" {
			continue
		}
		if !strings.HasPrefix(s, ".") {
			s = "." + s
		}
		out[s] = struct{}{}
	}
	return out
}

func buildIndex(root string, workers int, maxFileSize int64, allowedExt map[string]struct{}) (*Index, BuildStats, error) {
	if workers <= 0 {
		workers = 1
	}

	idx := &Index{
		Version:   1,
		Root:      root,
		CreatedAt: time.Now().Format(time.RFC3339),
		Docs:      make([]Document, 0, 1024),
		Inverted:  make(map[string][]Posting, 4096),
	}

	jobs := make(chan string, workers*4)
	var wg sync.WaitGroup
	var mu sync.Mutex
	stats := BuildStats{}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				tfMap, tokenCount, err := indexOneFile(path, maxFileSize)
				mu.Lock()
				stats.ScannedFiles++
				if err != nil || tokenCount == 0 {
					stats.SkippedFiles++
					mu.Unlock()
					continue
				}
				docID := len(idx.Docs)
				idx.Docs = append(idx.Docs, Document{Path: path, Length: tokenCount})
				for term, tf := range tfMap {
					idx.Inverted[term] = append(idx.Inverted[term], Posting{DocID: docID, TF: tf})
				}
				stats.IndexedFiles++
				mu.Unlock()
			}
		}()
	}

	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if _, ok := defaultIgnoredDirs[d.Name()]; ok {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		if len(allowedExt) > 0 {
			ext := strings.ToLower(filepath.Ext(path))
			if _, ok := allowedExt[ext]; !ok {
				return nil
			}
		}
		jobs <- path
		return nil
	})
	close(jobs)
	wg.Wait()
	if walkErr != nil {
		return nil, stats, walkErr
	}

	var totalLen int
	for _, d := range idx.Docs {
		totalLen += d.Length
	}
	idx.TotalDocs = len(idx.Docs)
	if idx.TotalDocs > 0 {
		idx.AvgDocLen = float64(totalLen) / float64(idx.TotalDocs)
	}
	return idx, stats, nil
}

func indexOneFile(path string, maxFileSize int64) (map[string]int, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	if st.Size() > maxFileSize {
		return nil, 0, fmt.Errorf("file too large")
	}

	r := bufio.NewReader(f)
	peek, _ := r.Peek(4096)
	if isLikelyBinary(peek) {
		return nil, 0, fmt.Errorf("binary file")
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, 0, err
	}
	tokens := tokenize(string(data))
	if len(tokens) == 0 {
		return nil, 0, nil
	}

	tf := make(map[string]int, len(tokens)/2+1)
	for _, t := range tokens {
		tf[t]++
	}
	return tf, len(tokens), nil
}

func tokenize(s string) []string {
	buf := make([]rune, 0, 64)
	out := make([]string, 0, 128)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		out = append(out, strings.ToLower(string(buf)))
		buf = buf[:0]
	}

	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			buf = append(buf, r)
		} else {
			flush()
		}
	}
	flush()
	return out
}

func isLikelyBinary(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	sample := b
	if len(sample) > 4096 {
		sample = sample[:4096]
	}
	for _, x := range sample {
		if x == 0 {
			return true
		}
	}
	return false
}

func saveIndex(path string, idx *Index) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(idx)
}

func loadIndex(path string) (*Index, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var idx Index
	if err := json.NewDecoder(f).Decode(&idx); err != nil {
		return nil, err
	}
	if idx.TotalDocs == 0 {
		idx.TotalDocs = len(idx.Docs)
	}
	if idx.AvgDocLen == 0 && len(idx.Docs) > 0 {
		var total int
		for _, d := range idx.Docs {
			total += d.Length
		}
		idx.AvgDocLen = float64(total) / float64(len(idx.Docs))
	}
	if idx.Inverted == nil {
		idx.Inverted = map[string][]Posting{}
	}
	return &idx, nil
}

type Result struct {
	DocID int
	Score float64
}

func bm25Query(idx *Index, query string, k1, b float64, topK int) []Result {
	if idx.TotalDocs == 0 || topK <= 0 {
		return nil
	}
	terms := tokenize(query)
	if len(terms) == 0 {
		return nil
	}

	scores := make(map[int]float64, 256)
	N := float64(idx.TotalDocs)
	avgDL := idx.AvgDocLen
	if avgDL <= 0 {
		avgDL = 1
	}

	for _, term := range terms {
		plist, ok := idx.Inverted[term]
		if !ok || len(plist) == 0 {
			continue
		}
		df := float64(len(plist))
		idf := math.Log(1 + (N-df+0.5)/(df+0.5))
		for _, p := range plist {
			dl := float64(idx.Docs[p.DocID].Length)
			tf := float64(p.TF)
			numer := tf * (k1 + 1)
			denom := tf + k1*(1-b+b*dl/avgDL)
			scores[p.DocID] += idf * (numer / denom)
		}
	}

	results := make([]Result, 0, len(scores))
	for docID, score := range scores {
		results = append(results, Result{DocID: docID, Score: score})
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score == results[j].Score {
			return idx.Docs[results[i].DocID].Path < idx.Docs[results[j].DocID].Path
		}
		return results[i].Score > results[j].Score
	})
	if len(results) > topK {
		results = results[:topK]
	}
	return results
}
