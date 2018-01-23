package app

import (
	"WarmupProject/database/pkg/datastorehandler"
	"WarmupProject/database/pkg/filehandler"
	"WarmupProject/database/pkg/processhandler"
	"WarmupProject/database/pkg/test"
	"net/http"
)

/* function to set up handlers */
func init() {
	http.HandleFunc("/csv", datastorehandler.CsvHandler)
	http.HandleFunc("/process", processhandler.QueryHandler)
	http.HandleFunc("/blob", filehandler.BlobHandler)
	http.HandleFunc("/upload", filehandler.UploadHandler)
	http.HandleFunc("/download", filehandler.DownloadHandler)
	http.HandleFunc("/test", test.TestHandler)
	http.HandleFunc("/avg", datastorehandler.ProcessHistogram)
	http.HandlerFunc("/avgdiff", datastorehandler.ProcessHistogramDiff)
}
