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
