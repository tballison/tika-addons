# org.apache.tika-addons
Addons not part of the official Tika release

This repo is designed to offer examples of parsers that will likely never belong in the main Tika distribution.

unravel
=======
Let's say you have a large pst/mbox/tar/zip file and you
want a tika extract for each embedded file, try unravel.

This is in no way ready for integration with Apache Tika, but it should offer the beginnings for feedback.

To run:
1. Build the unravel project and put the unravel.jar in a 'tika_bin' directory that also includes tika-app-1.17.jar
2. Commandline:
      `java -cp tika_bin/* org.tallison.tika.unravelers.UnravelCLI -i my_pst.pst -o extracts`

Lots more needs to be improved, but this is a first cut.
