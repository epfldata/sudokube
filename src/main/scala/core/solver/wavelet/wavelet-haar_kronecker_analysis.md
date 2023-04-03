`w(cuboid) -> wavelet` is the Haar transform function at `core.solver.wavelet.HaarTransformer::forward`.

#### For single dimension, the Haar transform is defined as:

```
A = [a, b]
w(A) = [a+b, a-b]
```

---

#### For two dimensions, the Haar transform is defined as:

```
AB = [a, b, c, d] = [00, 01, 10, 11]

A = [a+b, c+d]      =>      A' = w(A) = [a+b + c+d, a+b - c-d]
B = [a+c, b+d]      =>      B' = w(B) = [a+c + b+d, a+c - b-d]

w(AB) =  [a+b+c+d, a-b+c-d, a+b-c-d, a-b-c+d]
      => [  A'[0],   B'[1],   A'[1], f(AB) = a-b-c+d] 
      == [  B'[0],   B'[1],   A'[1], f(AB) = a-b-c+d]
```
```
BA = [a, c, b, d]

w(BA) =  [a+c+b+d, a-c+b-d, a+c-b-d, a-c-b+d]
      => [  A'[0],   A'[1],   B'[1], f(BA) == f(AB) = a-c-b+d] 

```
If matrix M is a permutation matrix, such that `M * AB = BA`, then `M * w(AB) = w(BA)`.

```
M = 1 0 0 0
    0 0 1 0
    0 1 0 0
    0 0 0 1
```

---

#### For three dimensions, the Haar transform is defined as:

```
ABC = [a, b, c, d, e, f, g, h] = [000, 001, 010, 011, 100, 101, 110, 111]

A = [a+b+c+d, e+f+g+h]      =>      A'  = w(A)  = [ a+b+c+d + e+f+g+h, a+b+c+d - e-f-g-h]
B = [a+b+e+f, c+d+g+h]      =>      B'  = w(B)  = [ a+b+e+f + c+d+g+h, a+b+e+f - c-d-g-h]
C = [a+c+e+g, b+d+f+h]      =>      C'  = w(C)  = [ a+c+e+g + b+d+f+h, a+c+e+g - b-d-f-h]

AB = [a+b, c+d, e+f, g+h]   =>      AB' = w(AB) = [a+b+c+d +e+f+g+h, a+b-c-d +e+f-g-h, a+b+c+d -e-f-g-h, a+b-c-d -e-f+g+h]
                                                =>[           A'[0],            B'[1],            A'[1], a+b-c-d -e-f+g+h]
BC = [a+e, b+f, c+g, d+h]   =>      BC' = w(BC) = [a+e+b+f +c+g+d+h, a+e-b-f +c+g-d-h, a+e+b+f -c-g-d-h, a+e-b-f -c-g+d+h]
                                                =>[           B'[0],            C'[1],            B'[1], a+e-b-f -c-g+d+h]
AC = [a+c, b+d, e+g, f+h]   =>      AC' = w(AC) = [a+c+b+d +e+g+f+h, a+c-b-d +e+g-f-h, a+c+b+d -e-g-f-h, a+c-b-d -e-g+f+h]
                                                =>[           A'[0],            C'[1],            A'[1], a+c-b-d -e-g+f+h]
                                                
w(ABC) =    [a+b+c+d +e+f+g+h, a-b+c-d +e-f+g-h, a+b-c-d +e+f-g-h, a-b-c+d +e-f-g+h, 
             a+b+c+d -e-f-g-h, a-b+c-d -e+f-g+h, a+b-c-d -e-f+g+h, a-b-c+d -e+f+g-h]
       
       =>   [A'[0], C'[1], B'[1], BC'[3],  
             A'[1], AC'[3], AB'[3], f(ABC) = a-b-c+d -e+f+g-h] 
       
```
```
BCA = [a, e, b, f, c, g, d, h]
w(BCA) =    [a+e+b+f +c+g+d+h, a-e+b-f +c-g+d-h, a+e-b-f +c+g-d-h, a-e-b+f +c-g-d+h, 
             a+e+b+f -c-g-d-h, a-e+b-f -c+g-d+h, a+e-b-f -c-g+d+h, a-e-b+f -c+g+d-h]  

         =>   [B'[0], A'[1], C'[1], AC'[3],  
               B'[1], AB'[3], BC'[3], f(BCA) == f(ABC) = a-e-b+f -c+g+d-h]                                            
               
```
```
BAC = [a, b, e, f, c, d, g, h]
w(BAC) =    [a+b+e+f +c+d+g+h, a-b+e-f +c-d+g-h, a+b-e+f +c+d-g-h, a-b-e-f +c-d-g+h, 
             a+b+e+f -c-d-g-h, a-b+e-f -c+d-g+h, a+b-e+f -c-d+g+h, a-b-e-f -c+d+g-h]  

         =>   [B'[0], C'[1], A'[1], AC'[3],  
               B'[1], BC'[3], AB'[3], f(BAC) == f(ABC) = a-b-e-f -c+d+g-h]                                            

```

Same principle applies,
If matrix M is a permutation matrix, such that `M * ABC = BCA`, then `M * w(ABC) = w(BCA)`.

#### For four dimensions, the Haar transform is defined as:

```
ABCD = [   a,    b,    c,    d,    e,    f,    g,    h,    i,    j,    k,    l,    m,    n,    o,    p]
       [0000, 0001, 0010, 0011, 0100, 0101, 0110, 0111, 1000, 1001, 1010, 1011, 1100, 1101, 1110, 1111]

A = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p]
B = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p]
C = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+e+f+i+j+m+n-c-d-g-h-k-l-o-p]
D = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+c+e+g+i+k+m+o-b-d-f-h-j-l-n-p]
AB = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p, +a+b+c+d+m+n+o+p-e-f-g-h-i-j-k-l]
AC = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+e+f+i+j+m+n-c-d-g-h-k-l-o-p, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p, +a+b+e+f+k+l+o+p-c-d-g-h-i-j-m-n]
AD = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+c+e+g+i+k+m+o-b-d-f-h-j-l-n-p, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p, +a+c+e+g+j+l+n+p-b-d-f-h-i-k-m-o]
BC = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+e+f+i+j+m+n-c-d-g-h-k-l-o-p, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p, +a+b+g+h+i+j+o+p-c-d-e-f-k-l-m-n]
BD = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+c+e+g+i+k+m+o-b-d-f-h-j-l-n-p, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p, +a+c+f+h+i+k+n+p-b-d-e-g-j-l-m-o]
CD = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+c+e+g+i+k+m+o-b-d-f-h-j-l-n-p, +a+b+e+f+i+j+m+n-c-d-g-h-k-l-o-p, +a+d+e+h+i+l+m+p-b-c-f-g-j-k-n-o]
ABC = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+e+f+i+j+m+n-c-d-g-h-k-l-o-p, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p, +a+b+g+h+i+j+o+p-c-d-e-f-k-l-m-n, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p, +a+b+e+f+k+l+o+p-c-d-g-h-i-j-m-n, +a+b+c+d+m+n+o+p-e-f-g-h-i-j-k-l, +a+b+g+h+k+l+m+n-c-d-e-f-i-j-o-p]
ABD = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+c+e+g+i+k+m+o-b-d-f-h-j-l-n-p, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p, +a+c+f+h+i+k+n+p-b-d-e-g-j-l-m-o, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p, +a+c+e+g+j+l+n+p-b-d-f-h-i-k-m-o, +a+b+c+d+m+n+o+p-e-f-g-h-i-j-k-l, +a+c+f+h+j+l+m+o-b-d-e-g-i-k-n-p]
ACD = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+c+e+g+i+k+m+o-b-d-f-h-j-l-n-p, +a+b+e+f+i+j+m+n-c-d-g-h-k-l-o-p, +a+d+e+h+i+l+m+p-b-c-f-g-j-k-n-o, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p, +a+c+e+g+j+l+n+p-b-d-f-h-i-k-m-o, +a+b+e+f+k+l+o+p-c-d-g-h-i-j-m-n, +a+d+e+h+j+k+n+o-b-c-f-g-i-l-m-p]
BCD = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+c+e+g+i+k+m+o-b-d-f-h-j-l-n-p, +a+b+e+f+i+j+m+n-c-d-g-h-k-l-o-p, +a+d+e+h+i+l+m+p-b-c-f-g-j-k-n-o, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p, +a+c+f+h+i+k+n+p-b-d-e-g-j-l-m-o, +a+b+g+h+i+j+o+p-c-d-e-f-k-l-m-n, +a+d+f+g+i+l+n+o-b-c-e-h-j-k-m-p]

```

```
w(ABCD) =  [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+c+e+g+i+k+m+o-b-d-f-h-j-l-n-p, +a+b+e+f+i+j+m+n-c-d-g-h-k-l-o-p, +a+d+e+h+i+l+m+p-b-c-f-g-j-k-n-o, 
            +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p, +a+c+f+h+i+k+n+p-b-d-e-g-j-l-m-o, +a+b+g+h+i+j+o+p-c-d-e-f-k-l-m-n, +a+d+f+g+i+l+n+o-b-c-e-h-j-k-m-p, 
            +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p, +a+c+e+g+j+l+n+p-b-d-f-h-i-k-m-o, +a+b+e+f+k+l+o+p-c-d-g-h-i-j-m-n, +a+d+e+h+j+k+n+o-b-c-f-g-i-l-m-p, 
            +a+b+c+d+m+n+o+p-e-f-g-h-i-j-k-l, +a+c+f+h+j+l+m+o-b-d-e-g-i-k-n-p, +a+b+g+h+k+l+m+n-c-d-e-f-i-j-o-p, +a+d+f+g+j+k+m+p-b-c-e-h-i-l-n-o]
        
        =  [A'[0], D'[1], C'[1], CD'[3], B'[1], BD'[3], BC'[3], BCD'[7], 
            A'[1], AD'[3], AC'[3], ACD'[7], AB'[3], ABD'[7], ABC'[7], f(ABCD) = +a+d+f+g+j+k+m+p-b-c-e-h-i-l-n-o]

```
