namespace MpInflux;

public static class Extensions
{
    public static List<List<T>> Batch<T>(this List<T> list, int size)
    {
        List<List<T>> newList = new List<List<T>>();
        for (int i = 0; i < list.Count; i++)
        {
            int groupNum = i / size;
            if (groupNum + 1 > newList.Count) newList.Add([]);
            newList[groupNum].Add(list[i]);
        }
        return newList;
    }
}
