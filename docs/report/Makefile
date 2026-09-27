# Build the report.
#
#   make          -> main.pdf
#   make clean    -> remove LaTeX build artefacts (keeps the PDF)
#
# Figures come from two generators, both re-runnable from the repo root:
#   make diagrams   draw.io sources in docs/diagrams -> vector PDFs
#   make figures    headless Chromium over the running stack -> PNGs

main.pdf: main.tex measurements.tex $(wildcard figures/*)
	latexmk -pdf -interaction=nonstopmode main.tex
	@echo "  pages: $$(pdfinfo main.pdf | awk '/^Pages/{print $$2}')"

.PHONY: clean distclean check
clean:
	@latexmk -c >/dev/null 2>&1 || true
	@rm -f *.aux *.log *.out *.toc *.fls *.fdb_latexmk *.synctex.gz

# Refuse to ship a report with unfilled cover-page placeholders.
check: main.pdf
	@if grep -qE 'EG/20XX|Group XX|TODO|XXXX' main.tex; then \
	  echo "  PLACEHOLDERS REMAIN in main.tex - fill the \\newcommand block"; \
	  grep -nE 'EG/20XX|Group XX|TODO|XXXX' main.tex | head; exit 1; \
	else echo "  no placeholders"; fi

distclean: clean
	@rm -f main.pdf
