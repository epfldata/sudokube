import {grpc} from "@improbable-eng/grpc-web";
import {Empty, BaseCuboidResponse} from "./_proto/sudokubeRPC_pb";
import {SudokubeService} from "./_proto/sudokubeRPC_pb_service";
const host =  "http://localhost:8081";

export function getBaseCuboids() {
    const arg =  new Empty();
    console.log("Calling RPC")
    grpc.unary(SudokubeService.getBaseCuboids, {
        request: arg,
        host: host,
        onEnd: res => {
            const { status, statusMessage, headers, message, trailers } = res;
            console.log("getBook.onEnd.status", status, statusMessage);
            console.log("getBook.onEnd.headers", headers);
            if (status === grpc.Code.OK && message) {
                console.log("getBook.onEnd.message", message.toObject());
            }
            console.log("getBook.onEnd.trailers", trailers);
        }
    });
}
