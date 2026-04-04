import React from 'react';

export function WhyNotScriptIt() {
  return (
    <section className="bg-bg-surface py-24 px-6 border-t border-border-subtle">
      <div className="max-w-5xl mx-auto flex flex-col items-center">
        <h2 className="text-[28px] md:text-[32px] font-semibold text-text-primary text-center mb-4 tracking-tight">
          You could write it yourself.
        </h2>
        <p className="text-[18px] text-text-secondary text-center mb-16 max-w-[500px]">
          Here's what Loafer handles instead.
        </p>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 w-full">
          {/* Custom Python Script */}
          <div className="flex flex-col">
            <h3 className="text-[14px] font-medium text-text-primary mb-4 flex items-center justify-between">
              <span>Standard Python script</span>
              <span className="text-red-400 text-xs px-2 py-0.5 bg-red-400/10 rounded-sm">40+ lines</span>
            </h3>
            <div className="bg-bg-elevated border border-border-default rounded-md p-4 flex-1 shadow-sm overflow-hidden select-none">
              <pre className="font-mono text-[12px] leading-[1.6] text-text-muted">
                <code>
<span className="text-text-secondary">import</span> psycopg2<br/>
<span className="text-text-secondary">import</span> csv<br/>
<span className="text-text-secondary">import</span> logging<br/>
<br/>
logging.basicConfig(level=logging.INFO)<br/>
<br/>
<span className="text-border-strong"># Setup connection, configure timeouts...</span><br/>
try:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;conn = psycopg2.connect(DATABASE_URL)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;cursor = conn.cursor('server_cursor')<br/>
&nbsp;&nbsp;&nbsp;&nbsp;<br/>
&nbsp;&nbsp;&nbsp;&nbsp;<span className="text-border-strong"># Execute with streaming support</span><br/>
&nbsp;&nbsp;&nbsp;&nbsp;cursor.execute("SELECT * FROM orders WHERE status = 'paid'")<br/>
&nbsp;&nbsp;&nbsp;&nbsp;<br/>
&nbsp;&nbsp;&nbsp;&nbsp;with open('output/orders.csv', 'w') as f:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;writer = csv.writer(f)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span className="text-border-strong"># Handle headers manually</span><br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;cols = [desc[0] for desc in cursor.description]<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;writer.writerow(cols)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span className="text-border-strong"># Read chunks to avoid OOM</span><br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;while True:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;records = cursor.fetchmany(size=5000)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if not records:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;break<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;for row in records:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span className="text-border-strong"># Format transformations, type conversions...</span><br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;writer.writerow(row)<br/>
except Exception as e:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;logging.error(f"Pipeline failed: {'{'}e{'}'}")<br/>
<span className="text-border-strong">... connections, rollbacks, error notifications</span>
                </code>
              </pre>
            </div>
          </div>

          {/* Loafer Config */}
          <div className="flex flex-col">
            <h3 className="text-[14px] font-medium text-text-primary mb-4 flex items-center justify-between">
              <span>Loafer configuration</span>
              <span className="text-green-400 text-xs px-2 py-0.5 bg-green-400/10 rounded-sm">8 lines</span>
            </h3>
            <div className="bg-bg-elevated border border-border-default rounded-md p-4 flex-1 shadow-sm overflow-hidden select-none">
              <pre className="font-mono text-[12px] leading-[1.6]">
                <code>
<span className="text-indigo-400">source:</span><br/>
  <span className="text-text-secondary">type:</span> <span className="text-green-400">postgres</span><br/>
  <span className="text-text-secondary">url:</span> <span className="text-text-code">${'{'}DATABASE_URL{'}'}</span><br/>
  <span className="text-text-secondary">query:</span> <span className="text-amber-400">"SELECT * FROM orders WHERE status = 'paid'"</span><br/>
<br/>
<span className="text-indigo-400">target:</span><br/>
  <span className="text-text-secondary">type:</span> <span className="text-green-400">csv</span><br/>
  <span className="text-text-secondary">path:</span> <span className="text-text-code">./output/orders.csv</span><br/>
                </code>
              </pre>
            </div>
          </div>
        </div>
        
        <div className="mt-12 text-center text-[14px] text-text-secondary max-w-2xl leading-[1.6]">
          Streaming batches, backpressure handling, incremental fetching, and schema inference are handled automatically. 
          You just describe the destination.
        </div>
      </div>
    </section>
  );
}
