import { Hero } from '../components/home/Hero'
import { DemoVideo } from '../components/home/DemoVideo';
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
      <div className="flex flex-col w-full">
        <Hero />
        <DemoVideo />
        <WhoItsFor />
        <HowItWorks />
        <TransformModes />
        <WhyNotScriptIt />
        <Connectors />
        <Scheduling />
        <DeployOptions />
        <OpenSource />
      </div>
  );
}
