`video.mp4` — синтетический двухсекундный ролик (testsrc), созданный для Begom.
Исходных материалов третьих лиц нет. Команда воспроизведения:

```sh
ffmpeg -f lavfi -i testsrc=duration=2:size=320x240:rate=12 \
  -pix_fmt yuv420p -c:v libx264 -an -movflags +faststart video.mp4
```
