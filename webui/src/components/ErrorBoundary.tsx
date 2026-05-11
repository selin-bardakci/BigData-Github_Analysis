import { Component, type ReactNode } from "react";
import { AlertTriangle } from "lucide-react";

type Props = { children: ReactNode };
type State = { err: Error | null };

export default class ErrorBoundary extends Component<Props, State> {
  state: State = { err: null };

  static getDerivedStateFromError(err: Error): State {
    return { err };
  }

  componentDidCatch(err: Error) {
    console.error("[ErrorBoundary]", err);
  }

  reset = () => this.setState({ err: null });

  render() {
    if (!this.state.err) return this.props.children;

    return (
      <div className="px-6 md:px-10 py-12">
        <div className="card card-pad max-w-2xl mx-auto">
          <div className="flex items-start gap-3">
            <AlertTriangle className="size-5 text-bad shrink-0 mt-0.5" />
            <div className="flex-1 min-w-0">
              <h2 className="text-base font-semibold mb-1">
                Bu bölüm yüklenirken bir hata oluştu
              </h2>
              <p className="text-sm text-sub mb-4">
                Diğer sayfalar etkilenmedi — sol menüden başka bir deliverable
                seçebilirsin.
              </p>
              <pre className="text-xs bg-surface border border-border rounded-lg p-3 overflow-auto max-h-48 font-mono text-sub">
                {this.state.err.message}
              </pre>
              <button onClick={this.reset} className="btn btn-primary mt-4">
                Tekrar dene
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }
}
