# Finddupes

## Compile
```cpp
clang++ -Wall -Wextra -fsanitize=address -std=c++2a \
-lboost_filesystem main.cpp && time ./a.out "/usr/local"
```

## Run
```cpp
./a.out "/usr/local"
```

