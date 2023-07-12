# Sudokube – Web Frontend Interface

## Introduction and references

* For an overview of the overall system and the usage of the frontend, see our demo paper and video at SIGMOD '23:

    Sachin Basil John, Peter Lindner, Zhekai Jiang, and Christoph Koch. 2023. Aggregation and Exploration of High-Dimensional Data Using the Sudokube Data Cube Engine. In *Companion of the 2023 International Conference on Management of Data (SIGMOD-Companion ’23), June 18–23, 2023, Seattle, WA, USA*. ACM, New York, NY, USA, 4 pages. https://doi.org/10.1145/3555041.3589729

* The core of the Sudokube project is located in the repository [epfldata/sudokube](https://github.com/epfldata/sudokube).

## Requirements

### Backend: solvers, storage, encoder, service, etc.

See [sudokube/epfldata/README.md](https://github.com/epfldata/sudokube/blob/main/README.md).

### Frontend: user interface

The frontend uses Node.js and npm. To install them on the machine, see https://nodejs.org/en/download/package-manager.

After cloning this repository, from the root of the web frontend, run the following to install dependencies and generate code (based on gRPC definitions, which are used to communicate with the backend service).
```
npm install
```

## Instructions to run

1. **Run the backend server**

    In the command line, from the root directory of the sudokube repository,

    1. Checkout the "demo" branch
        ```
        git checkout demo
        ```
        
    2. Run
        ```
        sbt "runMain frontend.service.SudokubeServer"
        ```

        The backend should then be hosted at localhost:8081.

2. **Run the frontend**

    With the backend running, from the root directory of the frontend, run
    ```
    npm start
    ```

    The frontend should then be hosted at localhost:3000 which you can visit in the browser to start exploring.
