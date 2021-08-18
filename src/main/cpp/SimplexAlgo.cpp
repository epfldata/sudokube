#include<stdio.h>
#include<assert.h>



class SimplexTableau {
public:
  const int ROWS, COLS;
  int **T, *base_v;

  SimplexTableau(int _ROWS, int _COLS) : ROWS(_ROWS), COLS(_COLS) {
    T = new int*[ROWS];
    for(int i = 0; i < ROWS; i++) T[i] = new int[COLS];
    base_v = new int[ROWS - 1];
  }

  ~SimplexTableau() {
    for(int i = 0; i < ROWS; i++) delete T[i];
    delete T;
    delete base_v;
  }

  void load(int *_T, int *_base_v) {
    for(int r = 0; r < ROWS - 1; r++) base_v[r] = _base_v[r];
    for(int r = 0; r < ROWS; r++)
      for(int c = 0; c < COLS; c++)
        T[r][c] = _T[r * COLS + c];
  }

  void axpy(int i_to, int i_from, int piv_col) {
    if(T[i_to][piv_col] != 0) {
      assert(T[i_from][piv_col] == 1); // or -1? 
      int factor = - T[i_to][piv_col];

      for(int c = 0; c < COLS; c++)
        T[i_to][c] = T[i_to][c] + T[i_from][c] * factor;

      assert(T[i_to][piv_col] == 0);
      assert((i_to == 0) || (T[i_to][COLS - 1] >= 0));
    }
  }

  void init() {
    for(int r = 0; r < ROWS - 1; r++) {
      if(T[0][base_v[r]] != 0)
        axpy(0, r + 1, base_v[r]);
    }
  }

  void tableau() {
    for(int r = 0; r < ROWS; r++) {
      for(int c = 0; c < COLS; c++)
        printf("%d ", T[r][c]);

      printf("\n");
    }
  }

  void pivot(int row, int col) {
    printf("pivot(%d, %d)\n", row, col);

    assert(T[row][col] > 0);
    assert(T[row][col] == 1);
    /*
    if(T[row][col] != 1)
      for(int c = 0; c < COLS; c++)
        T[row][c] = T[row][c] / T[row][col];
    */

    for(int i = 0; i < ROWS; i ++)
      if(row != i) axpy(i, row, col);

    base_v[row - 1] = col;
  }

  int pick_col() {
    for(int c = 0; c < COLS - 1; c ++)
      if(T[0][c] < 0) return c;

    return -1;
  }

  int pick_row(int col) {
    int best_v = -1;
    int best_r = -1;
    for(int i = 1; i < ROWS; i++) {
      if(T[i][col] > 0) {
        assert(T[i][COLS - 1] >= 0);
        assert(T[i][col] == 1);
        int ratio = T[i][COLS - 1];
        if((best_v == -1) || (ratio < best_r)) { best_v = i; best_r = ratio; }
      }
    }
    return best_v;
  }

  int algo() {
    int col = pick_col();
    while(col != -1) {
      int row = pick_row(col);
      if(row == -1) return -1;
      pivot(row, col);
      col = pick_col();
    }
    return T[0][COLS - 1];
  }
}; // end class SimplexTableau



int T0[] = {
// -1,  0, 1000, 0, 0,
  1, 0, 1000, 0, 0,
  1, -1, 1, 0, 2,
  1,  0, 0, 1, 10
};

int base_v0[] = { 2, 3 };



int main(void) {
  SimplexTableau *a = new SimplexTableau(3, 5);
  a->load(T0, base_v0);
  a->init();
  a->tableau();
  printf("%d\n", a->algo());

  return 0;
}


