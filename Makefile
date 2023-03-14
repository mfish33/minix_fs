.PHONY: all
all:: minls minget

minls: src/lib.rs src/bin/minls.rs
	cargo build -r --bin minls
	cp ./target/release/minls .

minget: src/lib.rs src/bin/minget.rs
	cargo build -r --bin minget
	cp ./target/release/minget .

clean:
	cargo clean
	rm ./minls ./minget
