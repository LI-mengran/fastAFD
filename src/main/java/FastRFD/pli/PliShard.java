package FastRFD.pli;

import java.util.List;

public class PliShard {

    public List<Pli> plis;

    /*
        tuple id range [beg, end)
    */
    public  int beg, end;

    public PliShard(List<Pli> plis, int beg, int end) {
        this.plis = plis;
        this.beg = beg;
        this.end = end;

        for (Pli pli : plis) {
            pli.setPlishard(this);
        }
    }

    public int getLength(){
        return end - beg;
    }

    public List<Pli> getPlis() {
        return plis;
    }
}
