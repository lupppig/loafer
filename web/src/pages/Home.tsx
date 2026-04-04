import React from 'react';
import { Helmet } from 'react-helmet-async';
import { Hero } from '../components/home/Hero';
import { WhoItsFor } from '../components/home/WhoItsFor';
import { HowItWorks } from '../components/home/HowItWorks';
import { TransformModes } from '../components/home/TransformModes';
import { WhyNotScriptIt } from '../components/home/WhyNotScriptIt';
import { Connectors } from '../components/home/Connectors';
import { Scheduling } from '../components/home/Scheduling';
import { DeployOptions } from '../components/home/DeployOptions';
import { OpenSource } from '../components/home/OpenSource';

export function Home() {
  return (
    <>
      <Helmet>
        <title>Loafer | Your data pipeline in plain English.</title>
        <meta name="description" content="Open source, AI-powered ELT CLI tool. Connect a source, describe your transformation, load clean data anywhere." />
      </Helmet>
      <div className="flex flex-col w-full">
        <Hero />
        <WhoItsFor />
        <HowItWorks />
        <TransformModes />
        <WhyNotScriptIt />
        <Connectors />
        <Scheduling />
        <DeployOptions />
        <OpenSource />
      </div>
    </>
  );
}
