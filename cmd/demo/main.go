// Demo app showing file upload/download with progress using Fyne and the uploader SDK.
package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/trendvidia/fyne/v2"
	"github.com/trendvidia/fyne/v2/app"
	"github.com/trendvidia/fyne/v2/container"
	"github.com/trendvidia/fyne/v2/data/binding"
	"github.com/trendvidia/fyne/v2/dialog"
	"github.com/trendvidia/fyne/v2/layout"
	"github.com/trendvidia/fyne/v2/widget"

	"github.com/trendvidia/uploader/sdk"
)

func main() {
	a := app.New()
	w := a.NewWindow("File Transfer Demo")
	w.Resize(fyne.NewSize(500, 340))

	client, err := sdk.New("localhost:9090")
	if err != nil {
		dialog.ShowError(fmt.Errorf("connect to server: %w", err), w)
	}

	// State: last uploaded file ID for download.
	var (
		mu         sync.Mutex
		lastFileID string
	)

	// --- Widgets ---
	// Use data bindings to avoid the Fyne textRenderer stale-bounds panic
	// that occurs when SetText changes text length during a layout pass.

	statusText := binding.NewString()
	statusText.Set("Ready")
	statusLabel := widget.NewLabelWithData(statusText)
	statusLabel.Truncation = fyne.TextTruncateEllipsis

	fileIDText := binding.NewString()
	fileIDText.Set("File ID: (none)")
	fileIDLabel := widget.NewLabelWithData(fileIDText)
	fileIDLabel.Truncation = fyne.TextTruncateEllipsis

	progressVal := binding.NewFloat()
	progressBar := widget.NewProgressBarWithData(progressVal)

	serverEntry := widget.NewEntry()
	serverEntry.SetText("localhost:9090")
	serverEntry.SetPlaceHolder("server:port")

	connectBtn := widget.NewButton("Connect", func() {
		if client != nil {
			client.Close()
		}
		c, err := sdk.New(serverEntry.Text)
		if err != nil {
			statusText.Set(fmt.Sprintf("Connection failed: %v", err))
			return
		}
		client = c
		statusText.Set("Connected to " + serverEntry.Text)
	})

	// --- Upload ---

	uploadBtn := widget.NewButton("Upload File...", func() {
		if client == nil {
			statusText.Set("Not connected")
			return
		}
		dialog.ShowFileOpen(func(reader fyne.URIReadCloser, err error) {
			if err != nil {
				dialog.ShowError(err, w)
				return
			}
			if reader == nil {
				return // cancelled
			}
			filePath := reader.URI().Path()
			fileName := reader.URI().Name()
			reader.Close()

			statusText.Set("Uploading: " + fileName)
			progressVal.Set(0)

			go func() {
				result, err := client.Upload(context.Background(), filePath,
					sdk.WithProgress(func(current, total int64) {
						if total > 0 {
							progressVal.Set(float64(current) / float64(total))
						}
					}),
				)
				if err != nil {
					fyne.Do(func() {
						statusText.Set(fmt.Sprintf("Upload failed: %v", err))
					})
					return
				}

				mu.Lock()
				lastFileID = result.FileID
				mu.Unlock()

				fyne.Do(func() {
					fileIDText.Set("File ID: " + result.FileID)
					if result.Complete {
						statusText.Set(fmt.Sprintf("Upload complete (%d bytes)", result.CommittedSize))
					} else {
						statusText.Set(fmt.Sprintf("Upload partial (%d bytes, session: %s)", result.CommittedSize, result.SessionID))
					}
				})
			}()
		}, w)
	})

	// --- Download ---

	downloadBtn := widget.NewButton("Download File...", func() {
		if client == nil {
			statusText.Set("Not connected")
			return
		}
		mu.Lock()
		fid := lastFileID
		mu.Unlock()
		if fid == "" {
			statusText.Set("No file to download — upload one first")
			return
		}

		dialog.ShowFileSave(func(writer fyne.URIWriteCloser, err error) {
			if err != nil {
				dialog.ShowError(err, w)
				return
			}
			if writer == nil {
				return // cancelled
			}
			destPath := writer.URI().Path()
			destName := writer.URI().Name()
			writer.Close()

			statusText.Set("Downloading to: " + destName)
			progressVal.Set(0)

			go func() {
				err := client.Download(context.Background(), fid, destPath,
					sdk.WithProgress(func(current, total int64) {
						if total > 0 {
							progressVal.Set(float64(current) / float64(total))
						}
					}),
				)
				if err != nil {
					fyne.Do(func() {
						statusText.Set(fmt.Sprintf("Download failed: %v", err))
					})
					return
				}
				fyne.Do(func() {
					statusText.Set("Download complete: " + destPath)
				})
			}()
		}, w)
	})

	// --- Layout ---

	serverRow := container.NewBorder(nil, nil, widget.NewLabel("Server:"), connectBtn, serverEntry)

	buttons := container.NewHBox(uploadBtn, downloadBtn)

	content := container.NewVBox(
		serverRow,
		widget.NewSeparator(),
		buttons,
		layout.NewSpacer(),
		fileIDLabel,
		progressBar,
		statusLabel,
	)

	w.SetContent(container.NewPadded(content))
	w.ShowAndRun()
}
