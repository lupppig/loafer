import React from 'react';
import { Database, BarChart2, FlaskConical, Code2 } from 'lucide-react';

export function WhoItsFor() {
  const personas = [
    {
      icon: <Database className="w-5 h-5 text-indigo-400" />,
      title: "Data Engineers",
      desc: "Stop writing boilerplate connectors and loading code. Bring your SQL or Python. Loafer handles the rest."
    },
    {
      icon: <BarChart2 className="w-5 h-5 text-indigo-400" />,
      title: "Data Analysts",
      desc: "Transform and load your data using clear natural language instructions. No Python required."
    },
    {
      icon: <FlaskConical className="w-5 h-5 text-indigo-400" />,
      title: "Data Scientists",
      desc: "Quickly move messy datasets into clean, structured formats before dropping them into your model training pipelines."
    },
    {
      icon: <Code2 className="w-5 h-5 text-indigo-400" />,
      title: "Backend Engineers",
      desc: "Move production database syncs out of application cron jobs and into robust, transparent ETL workflows."
    }
  ];

  return (
    <section className="bg-bg-base py-24 px-6 relative">
      <div className="max-w-4xl mx-auto flex flex-col items-center">
        <h2 className="text-[28px] md:text-[32px] font-semibold text-text-primary text-center mb-16 tracking-tight">
          Built for everyone who moves data.
        </h2>
        
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 w-full max-w-[800px]">
          {personas.map((p, i) => (
            <div 
              key={i} 
              className="group bg-bg-surface border border-border-subtle rounded-md p-6 hover:bg-bg-overlay hover:border-border-default transition-all duration-150 ease-out shadow-sm"
            >
              <div className="mb-4">{p.icon}</div>
              <h3 className="text-[15px] font-semibold font-sans text-text-primary mb-2">
                {p.title}
              </h3>
              <p className="text-[13px] text-text-secondary leading-[1.6]">
                {p.desc}
              </p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
