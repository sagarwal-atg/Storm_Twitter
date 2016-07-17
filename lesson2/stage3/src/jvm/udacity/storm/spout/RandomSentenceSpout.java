package udacity.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class RandomSentenceSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Random _rand;


  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
  }

  @Override
  public void nextTuple() {
    Utils.sleep(100);
    String[] sentences = new String[]{
      "  Pal bhar thahar jaao",
      "Dil ye sambhal jaaye  Kaise tumhe roka karun",
      "Meri taraf aata har gham phisal jaaye",
      "Aankhon mein tum ko bharun   Bin bole baatein tumse karun",
      "‘gar tum saath ho..   Agar tum saath ho ",
      " Behti rehti.. Nahar nadiya si teri duniya mein Meri duniya hai teri chaahaton mein",
"Main dhal jaati hoon teri aadaton mein gar tum saath ho",
"Teri nazron mein hai tere sapne Tere sapno mein hai naraazi",
"Mujhe lagta hai ke baatein dil ki Hoti lafzon ki dhokebaazi",
"Tum saath ho ya na ho kya fark hai Bedard thi zindagi bedard hai",
"Agar tum saath ho Agar tum saath ho",
"Palkein jhapakte hi din ye nikal jaaye Bethi bethi bhaagi phirun",
"Meri taraf aata har gham phisal jaaye Aankhon mein tum ko bharun",
"Bin bole baatein tumse karun ‘gar tum saath ho",
"Agar tum saath ho Teri nazron mein hai tere sapne",
"Tere sapno mein hai naraazi Mujhe lagta hai ke baatein dil ki",
"Hoti lafzon ki dhokebaazi Tum saath ho ya na ho kya fark hai",
"Bedard thi zindagi bedard hai Agar tum saath ho",
"Dil ye sambhal jaaye (Agar tum saath ho)",
"Har gham phisal jaaye "
      };
    String sentence = sentences[_rand.nextInt(sentences.length)];
    _collector.emit(new Values(sentence));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sentence"));
  }

}
