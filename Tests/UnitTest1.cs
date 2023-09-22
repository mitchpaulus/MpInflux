using MpInflux;

namespace Tests;

public class Tests
{
    [Test]
    public async Task Test1()
    {
        var input =
            "Fidelity WLK\n2022-09-01\n2023-09-22\n15\n/Fidelity Westlake-Grapevine ES/Trends/TDW/TDW 4th Floor E Wing/AHU_4A1/SA1_FL\n";
        var output = await Program.GetOutput(input);
    }
}