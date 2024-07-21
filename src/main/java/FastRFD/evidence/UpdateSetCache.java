package FastRFD.evidence;

import org.roaringbitmap.RoaringBitmap;

import java.util.HashMap;
import java.util.List;

public class UpdateSetCache<T> {
    int columnIndex;
    HashMap<T, List<RoaringBitmap>> updateSetCache = new HashMap<>();
    public UpdateSetCache() {

    }
    public void add(T object, List<RoaringBitmap> updateSet){
        updateSetCache.put(object, updateSet);
    }
    public boolean contain(T object){
        return updateSetCache.containsKey(object);
    }
    public List<RoaringBitmap> getUpdateSet(T object){
        return updateSetCache.get(object);
    }
    public void remove(T object){
        updateSetCache.remove(object);
    }
}
