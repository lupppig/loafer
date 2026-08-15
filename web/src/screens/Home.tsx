import { Hero } from '../components/home/Hero'
import { Anatomy } from '../components/home/Anatomy'
import { VideoDemo } from '../components/home/VideoDemo'
import { RunEvidence } from '../components/home/RunEvidence'
import { Authoring } from '../components/home/Authoring'
import { SourcesTargets } from '../components/home/SourcesTargets'
import { Operations } from '../components/home/Operations'
import { SelfHosting } from '../components/home/SelfHosting'
import { Faq } from '../components/home/Faq'
import { ProjectStatus } from '../components/home/ProjectStatus'

/*
 * Page order follows the narrative in the visual direction, with two additions.
 *
 * `Anatomy` sits directly under the hero because the page previously went from
 * a headline straight to a benchmark, which only works for a reader who
 * already knew what the tool was. `VideoDemo` follows it: explain in prose,
 * then show. `Faq` sits at the end, where the remaining objections live.
 */
export function Home() {
  return (
    <div className="flex w-full flex-col">
      <Hero />
      <Anatomy />
      <VideoDemo />
      <RunEvidence />
      <Authoring />
      <SourcesTargets />
      <Operations />
      <SelfHosting />
      <Faq />
      <ProjectStatus />
    </div>
  )
}
