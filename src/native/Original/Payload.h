#include <algorithm>


struct payload {
  private: unsigned lb;
  public:  unsigned sm;
  private: unsigned ub; // min, sum, max
  public:

  void merge_in(payload &other) {
    if(lb > ub) { // so far, an invalid interval
      lb = other.lb;
      ub = other.ub;
    }
    else if(other.lb > other.ub) {
      // nop
    }
    else {
      lb =  std::min(lb, other.lb);
      ub =  std::max(ub, other.ub);
    }
    sm +=                other.sm;
  }

  void copy_from(payload &other) {
    lb = other.lb;
    sm = other.sm;
    ub = other.ub;
  }

  // a sparse tuple is created
  void init_atomic(int v) { lb = v; sm = v; ub = v; }

  /* an empty dense array field is created
     now lb > ub, meaning that there is no interval
  */
  void init_empty() { lb = 3; sm = 0; ub = 0; }

//  payload() { init_empty(); }

  void print() { printf("(%u %u %u)", lb, sm, ub); }
};


