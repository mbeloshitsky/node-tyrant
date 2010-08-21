#
# Tokyo tyrant node.js connector makefile.
#

test: 
	cd tests; node basic.js

docs:
	naturaldocs -i . -o FramedHTML doc -p doc/project -q

