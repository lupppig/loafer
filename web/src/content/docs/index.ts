import type { ComponentType } from 'react'
import CliReference from './cli.mdx'
import DockerDocs from './docker.mdx'
import Installation from './installation.mdx'
import Introduction from './introduction.mdx'
import LlmSetup from './llms.mdx'
import Pipelines from './pipelines.mdx'
import Quickstart from './quickstart.mdx'
import SchedulingDocs from './scheduling.mdx'
import Sources from './sources.mdx'
import Targets from './targets.mdx'
import Transform from './transform.mdx'

type DocDefinition = {
  title: string
  description: string
  component: ComponentType
}

export const docs = {
  introduction: {
    title: 'Introduction',
    description: 'Understand Loafer, its audiences, and its core pipeline concepts.',
    component: Introduction,
  },
  installation: {
    title: 'Installation',
    description: 'Install the Loafer CLI and verify your environment.',
    component: Installation,
  },
  quickstart: {
    title: 'Quickstart',
    description: 'Build and run your first Loafer data pipeline.',
    component: Quickstart,
  },
  pipelines: {
    title: 'Pipeline configuration',
    description: 'Configure Loafer ETL and ELT pipelines with YAML.',
    component: Pipelines,
  },
  sources: {
    title: 'Source connectors',
    description: 'Configure databases, APIs, and files as Loafer sources.',
    component: Sources,
  },
  targets: {
    title: 'Target connectors',
    description: 'Configure durable destinations and write modes.',
    component: Targets,
  },
  transform: {
    title: 'Transform modes',
    description: 'Choose SQL, Python, or AI-assisted transformation modes.',
    component: Transform,
  },
  cli: {
    title: 'CLI reference',
    description: 'Reference for Loafer command-line workflows.',
    component: CliReference,
  },
  scheduling: {
    title: 'Scheduling',
    description: 'Schedule and manage recurring local pipeline runs.',
    component: SchedulingDocs,
  },
  docker: {
    title: 'Docker',
    description: 'Run Loafer reproducibly with Docker.',
    component: DockerDocs,
  },
  llms: {
    title: 'LLM setup',
    description: 'Configure optional LLM providers for AI-assisted transforms.',
    component: LlmSetup,
  },
} satisfies Record<string, DocDefinition>

export type DocSlug = keyof typeof docs
export const docSlugs = Object.keys(docs) as DocSlug[]

export function isDocSlug(value: string): value is DocSlug {
  return value in docs
}
