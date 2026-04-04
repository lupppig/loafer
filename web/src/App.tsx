import React from 'react';
import { Routes, Route, Outlet } from 'react-router-dom';
import { Topbar } from './components/layout/Topbar';
import { Footer } from './components/layout/Footer';
import { Home } from './pages/Home';
import { Docs } from './pages/Docs';
import { Changelog } from './pages/Changelog';

// MDX Components
import Introduction from './content/docs/introduction.mdx';
import Installation from './content/docs/installation.mdx';
import Quickstart from './content/docs/quickstart.mdx';
import Pipelines from './content/docs/pipelines.mdx';
import Sources from './content/docs/sources.mdx';
import Targets from './content/docs/targets.mdx';
import Transform from './content/docs/transform.mdx';
import CliReference from './content/docs/cli.mdx';
import SchedulingDocs from './content/docs/scheduling.mdx';
import DockerDocs from './content/docs/docker.mdx';

function RootLayout() {
  return (
    <div className="min-h-screen flex flex-col antialiased">
      <Topbar />
      <main className="flex-1 mt-[52px] flex flex-col w-full relative">
        <Outlet />
      </main>
      <Footer />
    </div>
  );
}

export default function App() {
  return (
    <Routes>
      <Route element={<RootLayout />}>
        <Route path="/" element={<Home />} />
        
        <Route path="/docs" element={<Docs />}>
          <Route path="introduction" element={<Introduction />} />
          <Route path="installation" element={<Installation />} />
          <Route path="quickstart" element={<Quickstart />} />
          <Route path="pipelines" element={<Pipelines />} />
          <Route path="sources" element={<Sources />} />
          <Route path="targets" element={<Targets />} />
          <Route path="transform" element={<Transform />} />
          <Route path="cli" element={<CliReference />} />
          <Route path="scheduling" element={<SchedulingDocs />} />
          <Route path="docker" element={<DockerDocs />} />
        </Route>

        <Route path="/changelog" element={<Changelog />} />
      </Route>
    </Routes>
  );
}
