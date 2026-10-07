package video

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

type Rendition struct {
	Name      string
	Width     int
	VideoRate string
	Bandwidth int
}

var defaultRenditions = []Rendition{
	{Name: "360p", Width: 640, VideoRate: "800k", Bandwidth: 950000},
	{Name: "720p", Width: 1280, VideoRate: "2800k", Bandwidth: 3100000},
	{Name: "1080p", Width: 1920, VideoRate: "5000k", Bandwidth: 5400000},
}

type Transcoder struct{ FFmpeg string }

func (t Transcoder) Transcode(ctx context.Context, job Job) error {
	ffmpeg := t.FFmpeg
	if ffmpeg == "" {
		ffmpeg = "ffmpeg"
	}
	temporary := job.OutputPath + ".work"
	if err := os.RemoveAll(temporary); err != nil {
		return err
	}
	if err := os.MkdirAll(temporary, 0o700); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errorsByRendition := make(chan error, len(defaultRenditions))
	var workers sync.WaitGroup
	for _, rendition := range defaultRenditions {
		workers.Add(1)
		go func(profile Rendition) {
			defer workers.Done()
			directory := filepath.Join(temporary, profile.Name)
			if err := os.MkdirAll(directory, 0o700); err != nil {
				errorsByRendition <- err
				cancel()
				return
			}
			command := exec.CommandContext(ctx, ffmpeg,
				"-hide_banner", "-loglevel", "error", "-y", "-i", job.InputPath,
				"-map", "0:v:0", "-map", "0:a:0?", "-vf", fmt.Sprintf("scale=%d:-2", profile.Width),
				"-c:v", "libx264", "-preset", "veryfast", "-b:v", profile.VideoRate,
				"-c:a", "aac", "-b:a", "128k", "-ac", "2",
				"-force_key_frames", "expr:gte(t,n_forced*6)", "-f", "hls", "-hls_time", "6",
				"-hls_playlist_type", "vod", "-hls_flags", "independent_segments",
				"-hls_segment_filename", filepath.Join(directory, "segment_%05d.ts"), filepath.Join(directory, "index.m3u8"))
			if output, err := command.CombinedOutput(); err != nil {
				errorsByRendition <- fmt.Errorf("%s transcode: %w: %s", profile.Name, err, output)
				cancel()
			}
		}(rendition)
	}
	workers.Wait()
	close(errorsByRendition)
	var failures []error
	for err := range errorsByRendition {
		failures = append(failures, err)
	}
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	if err := writeMasterPlaylist(temporary); err != nil {
		return err
	}
	if err := os.RemoveAll(job.OutputPath); err != nil {
		return err
	}
	return os.Rename(temporary, job.OutputPath)
}

func writeMasterPlaylist(root string) error {
	manifest := "#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-INDEPENDENT-SEGMENTS\n"
	for _, rendition := range defaultRenditions {
		manifest += fmt.Sprintf("#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%dx%d\n%s/index.m3u8\n",
			rendition.Bandwidth, rendition.Width, rendition.Width*9/16, rendition.Name)
	}
	return os.WriteFile(filepath.Join(root, "master.m3u8"), []byte(manifest), 0o600)
}
