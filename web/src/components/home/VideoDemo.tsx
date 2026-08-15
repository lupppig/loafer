'use client'

import { useRef, useState } from 'react'
import { Section, SectionHeading } from './Section'
import { absoluteUrl } from '../../lib/site'

/*
 * The demo, behind a facade.
 *
 * The video is a committed file rather than an embed, which means no third
 * party gets to watch this page load. The trade is that a 16:9 video element
 * with preload would be the heaviest thing above the fold, so nothing is
 * fetched until someone presses play: until then this is a poster image and a
 * button.
 *
 * The chapter list is not chrome. It is the transcript — the video has no
 * narration, so the written version of what happens in it has to exist
 * somewhere, and this is a place where it is useful to a reader who would
 * rather not watch anything at all.
 */

const SRC = '/media/loafer-demo.mp4'
const POSTER = '/media/loafer-demo-poster.jpg'
const DURATION_SECONDS = 51

const CHAPTERS = [
  { at: 3, label: 'Install', note: 'pip install loafer-etl, and a version check.' },
  {
    at: 7,
    label: 'Declare the pipeline',
    note: 'A source, a transformation, a target, and the execution policy — one file.',
  },
  {
    at: 21,
    label: 'Validate',
    note: 'Config, connections, and schema checked before a single row is read.',
  },
  {
    at: 25,
    label: 'Run',
    note: 'Thirty million rows move while resident memory stays flat under the cap.',
  },
  {
    at: 41,
    label: 'Reconcile',
    note: 'Input and output digests match, and the run publishes atomically.',
  },
] as const

function timecode(seconds: number): string {
  return `0:${String(seconds).padStart(2, '0')}`
}

export function VideoDemo() {
  const [playing, setPlaying] = useState(false)
  const video = useRef<HTMLVideoElement>(null)

  const start = (at?: number) => {
    setPlaying(true)
    // The element mounts in the same commit; seek and play on the next frame.
    requestAnimationFrame(() => {
      const node = video.current
      if (!node) return
      if (typeof at === 'number') node.currentTime = at
      void node.play().catch(() => {
        /* Autoplay refused: the controls are visible, so this is recoverable. */
      })
    })
  }

  const structuredData = {
    '@context': 'https://schema.org',
    '@type': 'VideoObject',
    name: 'How Loafer works: install, declare, validate, run, reconcile',
    description:
      'A walkthrough of a complete Loafer run: installing the CLI, declaring an ETL pipeline in YAML, validating it, moving thirty million rows at flat memory, and reconciling the input and output digests.',
    thumbnailUrl: [absoluteUrl(POSTER)],
    contentUrl: absoluteUrl(SRC),
    uploadDate: '2026-08-15',
    duration: `PT${DURATION_SECONDS}S`,
    isFamilyFriendly: true,
    inLanguage: 'en',
    hasPart: CHAPTERS.map((chapter, index) => ({
      '@type': 'Clip',
      name: chapter.label,
      startOffset: chapter.at,
      endOffset: CHAPTERS[index + 1]?.at ?? DURATION_SECONDS,
      url: `${absoluteUrl('/')}#demo`,
    })),
  }

  return (
    <Section id="demo" raised>
      <SectionHeading
        stamp="demo"
        title="Watch a run, start to finish"
        lede="Fifty seconds, no narration: the install, the pipeline file being written, validation, thirty million rows moving at flat memory, and the checksum at the end."
      />

      <div className="reveal mt-14 grid gap-10 lg:grid-cols-[1.45fr_0.55fr] lg:gap-14">
        <figure className="material-registration relative m-0 border border-steel bg-ink-base">
          <div className="relative aspect-video w-full overflow-hidden">
            {playing ? (
              <video
                ref={video}
                className="absolute inset-0 h-full w-full"
                src={SRC}
                poster={POSTER}
                controls
                playsInline
                preload="auto"
              >
                Your browser cannot play this video. The written walkthrough is beside it.
              </video>
            ) : (
              <button
                type="button"
                onClick={() => start()}
                className="group absolute inset-0 h-full w-full cursor-pointer"
                aria-label="Play the Loafer demo, 51 seconds, no sound"
              >
                {/* eslint-disable-next-line @next/next/no-img-element */}
                <img
                  src={POSTER}
                  alt="A Loafer run in progress: batches committing on the left, rows climbing to thirty million while resident memory holds flat on the right."
                  width={1920}
                  height={1080}
                  className="absolute inset-0 h-full w-full object-cover"
                  loading="lazy"
                  decoding="async"
                />
                <span className="absolute inset-0 bg-ink-base/35 transition-colors group-hover:bg-ink-base/20" />
                <span className="absolute inset-0 flex items-center justify-center">
                  <span className="flex h-16 w-16 items-center justify-center rounded-sm border border-signal bg-ink-base/85 transition-colors group-hover:bg-signal">
                    <svg
                      viewBox="0 0 24 24"
                      className="ml-0.5 h-6 w-6 fill-signal transition-colors group-hover:fill-ink-base"
                      aria-hidden="true"
                    >
                      <path d="M8 5v14l11-7z" />
                    </svg>
                  </span>
                </span>
              </button>
            )}
          </div>
          <figcaption className="flex items-center justify-between border-t border-steel-subtle px-4 py-2.5">
            <span className="stamp">loafer run — recorded walkthrough</span>
            <span className="stamp text-paper-mute">0:51 · no sound</span>
          </figcaption>
        </figure>

        <ol className="flex flex-col border-t border-steel-subtle">
          {CHAPTERS.map((chapter) => (
            <li key={chapter.label} className="border-b border-steel-subtle">
              <button
                type="button"
                onClick={() => start(chapter.at)}
                className="group w-full cursor-pointer py-4 text-left"
              >
                <span className="flex items-baseline gap-3">
                  <span className="figure text-[12px] text-signal">{timecode(chapter.at)}</span>
                  <span className="text-[15px] font-semibold tracking-[-0.01em] text-paper group-hover:text-signal">
                    {chapter.label}
                  </span>
                </span>
                <span className="mt-2 block text-[13px] leading-[1.6] text-paper-dim">
                  {chapter.note}
                </span>
              </button>
            </li>
          ))}
        </ol>
      </div>

      <script
        type="application/ld+json"
        // Built from CHAPTERS above, which also renders the visible list.
        dangerouslySetInnerHTML={{ __html: JSON.stringify(structuredData) }}
      />
    </Section>
  )
}
