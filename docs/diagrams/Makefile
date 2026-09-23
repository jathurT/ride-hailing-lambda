# Rebuild every report diagram from source.
#
# TikZ rather than draw.io: the report is LaTeX, so these compile to vector art with
# no external asset pipeline, fonts match the body text exactly, and the source sits
# in version control beside the code it describes. A diagram that can be regenerated
# cannot silently drift from the system.
#
#   make          build all PDFs
#   make preview  also render PNGs for eyeballing
#   make clean    remove build artefacts (keeps the PDFs)

TEX  := $(wildcard D*.tex)
PDF  := $(TEX:.tex=.pdf)
PNG  := $(TEX:.tex=-preview-1.png)

.PHONY: all preview clean distclean
all: $(PDF)

%.pdf: %.tex fleetstyle.tex
	pdflatex -interaction=nonstopmode -halt-on-error $< >/dev/null
	@echo "  built $@"

preview: $(PDF)
	@for f in $(PDF); do pdftoppm -png -r 130 $$f $${f%.pdf}-preview; done
	@echo "  previews rendered"

clean:
	@rm -f *.aux *.log *.out *.synctex.gz *-preview-*.png
	@echo "  cleaned (PDFs kept)"

distclean: clean
	@rm -f $(PDF)
