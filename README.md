# parso
Port of the parso SAS dataset parser from EPAM (https://github.com/epam/parso). 

The goals of the project area: 

1. Write a native scala library so no java dependencies are required
2. Implement multiparallel reading
3. Migrate to use cats or scalaz
4. Create a functional stream (fs) library for reading SAS files
