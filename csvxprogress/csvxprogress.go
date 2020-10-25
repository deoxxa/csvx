package csvxprogress

import (
	"fmt"
	"os"
	"time"

	"github.com/mitchellh/ioprogress"
	"github.com/pkg/errors"

	"fknsrs.biz/p/csvx"
)

type canStat interface {
	Stat() (os.FileInfo, error)
}

func WithProgress() csvx.Option {
	return WithProgressWindow(30)
}

func WithProgressWindow(window int) csvx.Option {
	return func(rd *csvx.Reader) error {
		fd, ok := rd.FD.(canStat)
		if !ok {
			return nil
		}

		st, err := fd.Stat()
		if err != nil {
			return errors.Wrap(err, "csvxprogress.WithProgressWindow")
		}

		rd.FD = &ioprogress.Reader{
			Reader:   rd.FD,
			Size:     st.Size(),
			DrawFunc: ioprogress.DrawTerminalf(os.Stderr, timeRemainingFormatter(window)),
		}

		return nil
	}
}

var byteUnits = []string{"B", "KB", "MB", "GB", "TB", "PB"}

func byteUnitStr(n int64) string {
	var unit string
	size := float64(n)
	for i := 1; i < len(byteUnits); i++ {
		if size < 1000 {
			unit = byteUnits[i-1]
			break
		}

		size = size / 1000
	}

	return fmt.Sprintf("%.3g %s", size, unit)
}

func timeRemainingFormatter(c int) ioprogress.DrawTextFormatFunc {
	var (
		rates        = make([]int64, c)
		startTime    = time.Now()
		lastTime     = startTime
		lastProgress int64
	)

	var i int
	return func(progress, total int64) string {
		thisTime := time.Now()
		block := progress - lastProgress

		dur := thisTime.Sub(lastTime)
		if dur != 0 {
			rates[i%c] = int64(float64(block) / float64(dur/time.Second))
			i++
		}

		lastTime = thisTime
		lastProgress = progress

		var averageRate, j int64
		for _, r := range rates {
			if r == 0 {
				continue
			}

			averageRate += r

			j++
		}
		if j != 0 {
			averageRate /= j
		}

		var remaining time.Duration
		if averageRate != 0 {
			remaining = time.Duration((total-progress)/averageRate) * time.Second
		}

		if progress == total {
			return fmt.Sprintf("read %s in %s", byteUnitStr(total), time.Now().Sub(startTime))
		}

		return fmt.Sprintf("%s/%s (%s/s; %s estimated)", byteUnitStr(progress), byteUnitStr(total), byteUnitStr(averageRate), remaining)
	}
}
