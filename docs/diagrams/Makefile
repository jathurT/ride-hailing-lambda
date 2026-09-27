# Export the draw.io diagrams to vector PDF and copy them to the report's figures.
#   make            regenerate *.drawio from build_drawio.py, then export
# The .drawio files can also be opened and edited in draw.io directly.

FIGURES_DIR := ../report/figures
DIAGRAMS := D1-context D2-layered-architecture D3-event-sequence D4-merge-boundary \
            D5-storage D6-daily-batch
PDFS := $(patsubst %,%.pdf,$(DIAGRAMS))

.PHONY: all sources clean

all: sources $(PDFS)
	@mkdir -p $(FIGURES_DIR)
	@cp $(PDFS) $(FIGURES_DIR)/
	@echo "copied $(words $(PDFS)) diagrams to $(FIGURES_DIR)"

sources:
	../../.venv/bin/python build_drawio.py

%.pdf: %.drawio
	drawio -x -f pdf --crop -o $@ $< --no-sandbox >/dev/null 2>&1

clean:
	@rm -f *.pdf
