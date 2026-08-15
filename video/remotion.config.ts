import { Config } from '@remotion/cli/config'

/*
 * The demo is almost entirely flat colour and text, which compresses
 * extremely well and aliases badly. Overriding the scale is not worth it, but
 * a high-quality H.264 profile is: the YAML and the terminal output have to
 * stay readable after the browser scales the video down into a 16:9 figure.
 */

Config.setVideoImageFormat('jpeg')
Config.setJpegQuality(95)
Config.setChromiumOpenGlRenderer('angle')
Config.setOverwriteOutput(true)
