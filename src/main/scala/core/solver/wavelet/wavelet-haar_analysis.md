```
A = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p/2, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p/2]
B = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p/2, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p/2]
C = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p/2, +a+b+e+f+i+j+m+n-c-d-g-h-k-l-o-p/2]
D = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p/2, +a+c+e+g+i+k+m+o-b-d-f-h-j-l-n-p/2]

AB = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p/4, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p/4, +a+b+c+d-e-f-g-h/2, +i+j+k+l-m-n-o-p/2]
BA = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p/4, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p/4, +a+b+c+d-i-j-k-l/2, +e+f+g+h-m-n-o-p/2]
BD = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p/4, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p/4, +a+c+i+k-b-d-j-l/2, +e+g+m+o-f-h-n-p/2]
CD = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p/4, +a+b+e+f+i+j+m+n-c-d-g-h-k-l-o-p/4, +a+e+i+m-b-f-j-n/2, +c+g+k+o-d-h-l-p/2]

ABC = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p/8, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p/8, +a+b+c+d-e-f-g-h/4, +i+j+k+l-m-n-o-p/4, +a+b-c-d/2, +e+f-g-h/2, +i+j-k-l/2, +m+n-o-p/2]
BAC = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p/8, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p/8, +a+b+c+d-i-j-k-l/4, +e+f+g+h-m-n-o-p/4, +a+b-c-d/2, +i+j-k-l/2, +e+f-g-h/2, +m+n-o-p/2]
BCA = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p/8, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p/8, +a+b+i+j-c-d-k-l/4, +e+f+m+n-g-h-o-p/4, +a+b-i-j/2, +c+d-k-l/2, +e+f-m-n/2, +g+h-o-p/2]

ABD = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p/8, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p/8, +a+b+c+d-e-f-g-h/4, +i+j+k+l-m-n-o-p/4, +a+c-b-d/2, +e+g-f-h/2, +i+k-j-l/2, +m+o-n-p/2]
ACD = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p/8, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p/8, +a+b+e+f-c-d-g-h/4, +i+j+m+n-k-l-o-p/4, +a+e-b-f/2, +c+g-d-h/2, +i+m-j-n/2, +k+o-l-p/2]
BCD = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p/8, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p/8, +a+b+i+j-c-d-k-l/4, +e+f+m+n-g-h-o-p/4, +a+i-b-j/2, +c+k-d-l/2, +e+m-f-n/2, +g+o-h-p/2]

ABCD = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p/16, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p/16, +a+b+c+d-e-f-g-h/8, +i+j+k+l-m-n-o-p/8, +a+b-c-d/4, +e+f-g-h/4, +i+j-k-l/4, +m+n-o-p/4, +a-b/2, +c-d/2, +e-f/2, +g-h/2, +i-j/2, +k-l/2, +m-n/2, +o-p/2]

```
```
A = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p]
B = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p]
C = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+e+f+i+j+m+n-c-d-g-h-k-l-o-p]
D = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+c+e+g+i+k+m+o-b-d-f-h-j-l-n-p]
AB = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p, +a+b+c+d-e-f-g-h, +i+j+k+l-m-n-o-p]
BA = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p, +a+b+c+d-i-j-k-l, +e+f+g+h-m-n-o-p]
BD = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p, +a+c+i+k-b-d-j-l, +e+g+m+o-f-h-n-p]
CD = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+e+f+i+j+m+n-c-d-g-h-k-l-o-p, +a+e+i+m-b-f-j-n, +c+g+k+o-d-h-l-p]
ABC = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p, +a+b+c+d-e-f-g-h, +i+j+k+l-m-n-o-p, +a+b-c-d, +e+f-g-h, +i+j-k-l, +m+n-o-p]
BAC = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p, +a+b+c+d-i-j-k-l, +e+f+g+h-m-n-o-p, +a+b-c-d, +i+j-k-l, +e+f-g-h, +m+n-o-p]
BCA = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p, +a+b+i+j-c-d-k-l, +e+f+m+n-g-h-o-p, +a+b-i-j, +c+d-k-l, +e+f-m-n, +g+h-o-p]
ABD = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p, +a+b+c+d-e-f-g-h, +i+j+k+l-m-n-o-p, +a+c-b-d, +e+g-f-h, +i+k-j-l, +m+o-n-p]
ACD = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p, +a+b+e+f-c-d-g-h, +i+j+m+n-k-l-o-p, +a+e-b-f, +c+g-d-h, +i+m-j-n, +k+o-l-p]
BCD = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+c+d+i+j+k+l-e-f-g-h-m-n-o-p, +a+b+i+j-c-d-k-l, +e+f+m+n-g-h-o-p, +a+i-b-j, +c+k-d-l, +e+m-f-n, +g+o-h-p]
ABCD = [+a+b+c+d+e+f+g+h+i+j+k+l+m+n+o+p, +a+b+c+d+e+f+g+h-i-j-k-l-m-n-o-p, +a+b+c+d-e-f-g-h, +i+j+k+l-m-n-o-p, +a+b-c-d, +e+f-g-h, +i+j-k-l, +m+n-o-p, +a-b, +c-d, +e-f, +g-h, +i-j, +k-l, +m-n, +o-p]

```

```
AB = [A'[0], A'[1], f(B'[1]), g(B'[1])], f(B'[1]) + g(B'[1]) = B'[1]
BA = [B'[0], B'[1], f(A'[1]), g(A'[1])], f(A'[1]) + g(A'[1]) = A'[1]

AB = [A'[0]/2, A'[1]/2, f(B'[1]), g(B'[1])], f(B'[1]) + g(B'[1]) = B'[1]


AC = [A'[0]/2, A'[1]/2, +a+b+e+f-c-d-g-h, +i+j+m+n-k-l-o-p] 
```

```
                                     |                C'[1]               |
                   |      B'[1]      |  (AC|BC)'[3]     |   (AC|BC)'[4]   |
ABC = [A'[0], A'[1], AB'[2], AB'[3], +a+b-c-d, +e+f-g-h, +i+j-k-l, +m+n-o-p]



                                     |                C'[1]               |
                   |      A'[1]      |  (AC|BC)'[3]     |   (AC|BC)'[4]   |
BAC = [B'[0], B'[1], BA'[2], BA'[3], +a+b-c-d, +i+j-k-l, +e+f-g-h, +m+n-o-p]

                                     |                A'[1]               |
                   |      C'[1]      |  (BA|CA)'[3]     |   (BA|CA)'[4]   |
BCA = [B'[0], B'[1], BC'[2], BC'[3], +a+b-i-j, +c+d-k-l, +e+f-m-n, +g+h-o-p]

```