package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

func main() {
	schedulerAddr := flag.String("scheduler", "http://localhost:8080", "scheduler address")
	flag.Parse()

	base := *schedulerAddr
	client := &http.Client{Timeout: 5 * time.Second}

	fmt.Println("=======================================")
	fmt.Println("  Task Scheduler Client Demo")
	fmt.Println("=======================================")

	tasks := []map[string]interface{}{
		{"name": "send_email", "payload": `{"to":"alice@example.com","subject":"Welcome!"}`, "priority": 10, "max_retries": 3, "timeout_seconds": 30},
		{"name": "process_image", "payload": `{"url":"https://example.com/photo.jpg","size":"1024x768"}`, "priority": 5, "max_retries": 2, "timeout_seconds": 60},
		{"name": "generate_report", "payload": `{"type":"monthly","month":"2026-02"}`, "priority": 8, "max_retries": 1, "timeout_seconds": 15},
		{"name": "send_email", "payload": `{"to":"bob@example.com","subject":"Invoice #1234"}`, "priority": 3, "max_retries": 3, "timeout_seconds": 30},
		{"name": "process_image", "payload": `{"url":"https://example.com/avatar.png","size":"256x256"}`, "priority": 7, "max_retries": 2, "timeout_seconds": 45},
	}

	fmt.Println("\nSubmitting tasks...")
	for _, task := range tasks {
		body, _ := json.Marshal(task)
		resp, err := client.Post(base+"/api/tasks", "application/json", bytes.NewReader(body))
		if err != nil {
			log.Printf("  Failed to submit task: %v", err)
			continue
		}
		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		fmt.Printf("  Submitted: id=%s name=%s priority=%v\n", result["id"], result["name"], result["priority"])
	}

	fmt.Println("\nMonitoring task progress...")
	for i := 0; i < 10; i++ {
		time.Sleep(3 * time.Second)
		resp, err := client.Get(base + "/api/stats")
		if err != nil {
			log.Printf("  Stats error: %v", err)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var stats map[string]interface{}
		json.Unmarshal(body, &stats)
		fmt.Printf("  [%s] pending=%v running=%v completed=%v failed=%v timeout=%v workers=%v\n",
			time.Now().Format("15:04:05"),
			stats["pending_tasks"], stats["running_tasks"],
			stats["completed_tasks"], stats["failed_tasks"],
			stats["timeout_tasks"], stats["active_workers"])
		pending, _ := stats["pending_tasks"].(float64)
		running, _ := stats["running_tasks"].(float64)
		if pending == 0 && running == 0 {
			fmt.Println("  All tasks processed!")
			break
		}
	}

	fmt.Println("\nFinal task states:")
	resp, err := client.Get(base + "/api/tasks")
	if err != nil {
		log.Fatalf("  Failed to list tasks: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var taskList []map[string]interface{}
	json.Unmarshal(body, &taskList)
	for _, t := range taskList {
		fmt.Printf("  %-20s %-18s status=%-10s retries=%v/%v\n",
			t["id"], t["name"], t["status"], t["retry_count"], t["max_retries"])
		if e, ok := t["error"].(string); ok && e != "" {
			fmt.Printf("    error: %s\n", e)
		}
		if r, ok := t["result"].(string); ok && r != "" {
			fmt.Printf("    result: %s\n", r)
		}
	}

	fmt.Println("\nWorkers:")
	resp, err = client.Get(base + "/api/workers")
	if err != nil {
		log.Fatalf("  Failed to list workers: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	var workers []map[string]interface{}
	json.Unmarshal(body, &workers)
	for _, w := range workers {
		fmt.Printf("  %s  status=%-8s  completed=%v  failed=%v\n",
			w["id"], w["status"], w["tasks_completed"], w["tasks_failed"])
	}

	fmt.Println("\n=======================================")
}
