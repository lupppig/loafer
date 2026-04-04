import React from 'react';

export function DeployOptions() {
  return (
    <section className="bg-bg-base py-24 px-6 border-t border-border-subtle">
      <div className="max-w-5xl mx-auto flex flex-col items-center">
        <h2 className="text-[28px] md:text-[32px] font-semibold text-text-primary text-center mb-6 tracking-tight">
          Deploy Anywhere
        </h2>
        <p className="text-[16px] text-text-secondary text-center max-w-2xl mb-16 leading-[1.6]">
          Loafer is built to adapt. Run it natively as a Python executable locally or dispatch it seamlessly through container clusters with our minimal image.
        </p>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 w-full">
          {/* Local Runtime */}
          <div className="flex flex-col bg-bg-elevated border border-border-default rounded-md p-6 shadow-sm overflow-hidden min-w-0">
            <h3 className="text-[13px] font-medium text-text-primary mb-4 uppercase tracking-wider flex items-center gap-2">
              <span className="w-2 h-2 rounded-full bg-indigo-500" />
              Local Runtime
            </h3>
            <p className="text-[14px] text-text-secondary mb-6 leading-relaxed">
              Install the lightweight CLI to execute pipelines directly. Perfect for cron scheduling and standalone scripts.
            </p>
            <div className="bg-bg-base border border-border-default rounded-md p-4 font-mono text-[12px] text-text-code leading-[1.7] overflow-x-auto min-w-0 whitespace-pre">
              <span className="text-text-primary">pip install loafer-etl && loafer run pipeline.yaml</span>
            </div>
          </div>

          {/* Docker Container */}
          <div className="flex flex-col bg-bg-elevated border border-border-default rounded-md p-6 shadow-sm overflow-hidden min-w-0">
            <h3 className="text-[13px] font-medium text-text-primary mb-4 uppercase tracking-wider flex items-center gap-2">
              <span className="w-2 h-2 rounded-full bg-cyan-500" />
              Docker Container
            </h3>
            <p className="text-[14px] text-text-secondary mb-6 leading-relaxed">
              Pull the official image and mount your configuration directories to keep your host environment completely clean.
            </p>
            <div className="bg-bg-base border border-border-default rounded-md p-4 font-mono text-[12px] text-text-code leading-[1.7] overflow-x-auto min-w-0 whitespace-pre">
              <span className="text-text-primary">docker run --rm -v $(pwd):/workspace -w /workspace ghcr.io/lupppig/loafer:latest run pipeline.yaml</span>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
