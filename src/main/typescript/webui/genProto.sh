echo "Generating classes from RPC protocol definitions..."
OUTDIR=src/_proto
mkdir -p ${OUTDIR}
./node_modules/protoc/bin/protoc \
	--proto_path=protos \
	--plugin=protoc-gen-ts=./node_modules/.bin/protoc-gen-ts \
	--plugin=protoc-gen-js=./node_modules/.bin/protoc-gen-js \
	--js_out=import_style=commonjs,binary:${OUTDIR} \
	--ts_out=service=grpc-web:${OUTDIR} \
	sudokubeRPC.proto
echo "Done"