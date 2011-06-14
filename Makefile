.SUFFIXES: .erl .beam

.erl.beam:
	erlc -Wall +debug_info $<

ERL = erl -boot start_clean

# Modules to compile
MODS = replica elastic_replica repobj core\
		 kv_core kvs kvstracker\
		 utils

all: compile

compile: ${MODS:%=%.beam}

clean:
	rm -rf *.beam erl_crash.dump
