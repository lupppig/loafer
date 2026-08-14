import { Hero } from '../components/home/Hero'
import { RunEvidence } from '../components/home/RunEvidence'
import { Authoring } from '../components/home/Authoring'
import { SourcesTargets } from '../components/home/SourcesTargets'
import { Operations } from '../components/home/Operations'
import { SelfHosting } from '../components/home/SelfHosting'
import { ProjectStatus } from '../components/home/ProjectStatus'

export function Home() {
  return (
    <div className="flex w-full flex-col">
      <Hero />
      <RunEvidence />
      <Authoring />
      <SourcesTargets />
      <Operations />
      <SelfHosting />
      <ProjectStatus />
    </div>
  )
}
