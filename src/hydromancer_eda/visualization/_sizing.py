"""Size the existing Plotly widget after its notebook container is attached.

FigureWidget versions do not consistently observe parent resizes. This zero-height
companion measures only the available width; all rendering stays in FigureWidget.
"""

from anywidget import AnyWidget
from traitlets import Int


class BookSizer(AnyWidget):
    measured_width = Int(480).tag(sync=True)
    maximum = Int(480).tag(sync=True)

    _esm = """
    export function render({ model, el }) {
        el.style.cssText = 'width:100%;height:0;min-height:0;flex:none;overflow:hidden';
        el.setAttribute('aria-hidden', 'true');
        let frame;
        const observer = new ResizeObserver(([entry]) => {
            cancelAnimationFrame(frame);
            frame = requestAnimationFrame(() => {
                // Below 320px the surrounding output scrolls rather than crushing
                // the three numeric columns (e.g. Jupyter's open mobile sidebar).
                const width = Math.max(320, Math.min(model.get('maximum'),
                    Math.floor(entry.contentRect.width)));
                if (width !== model.get('measured_width')) {
                    model.set('measured_width', width);
                    model.save_changes();
                }
            });
        });
        observer.observe(el);
        return () => {
            observer.disconnect();
            cancelAnimationFrame(frame);
        };
    }
    """
