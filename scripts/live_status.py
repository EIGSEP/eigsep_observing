from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import flask

from eigsep_observing import EigsepRedis

# XXX
# need somewhere to set the pairs, see the liveplotter script
PAIRS = ["0"]
# XXX

parser = ArgumentParser(
    description="Live status server for Eigsep",
    formatter_class=ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "--rpi-ip",
    dest="rpi_ip",
    default="10.10.10.10",
    help="IP address of the Raspberry Pi",
)
args = parser.parse_args()

app = flask.Flask(__name__)
r = EigsepRedis(host=args.rpi_ip)


@app.route("/")
def index():
    metadata = r.get_live_metadata()
    data = {}
    for p in PAIRS:
        data[p] = r.get_raw(f"data:{p}")
        # XXX need to process this somehow, unpack bytes ETC
    return flask.render_template("index.html", metadata=metadata, data=data)

if __name__=="__main__":
    print("Starting live status server, go to http://localhost:5000")
    app.run(host="localhost", port=5000, debug=True)
