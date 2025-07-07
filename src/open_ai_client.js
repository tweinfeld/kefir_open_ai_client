import pipe from "lodash/fp/pipe.js";
import get from "lodash/fp/get.js";
import noop from "lodash/fp/noop.js";
import identity from "lodash/fp/identity.js";
import toString from "lodash/fp/toString.js";
import concatStream from "concat-stream";
import kefir from "kefir";
import got from "got";
import split from "split";
import { pipeline } from "node:stream";

const DEFAULT_VERB = "POST";
const API_URL_BASE = "https://api.openai.com/v1";

export const OaiClientFactory = (apiKey) => {
  return (pathAndVerb, body = {}) => {
    const [verb, path] = [DEFAULT_VERB, ...pathAndVerb.split(" ")].slice(-2);
    return kefir.fromNodeCallback((cb) =>
      got({
        method: verb,
        url: [API_URL_BASE, path].join("/"),
        headers: {
          Authorization: ["Bearer", apiKey].join(" "),
        },
        resolveBodyOnly: true,
        responseType: "json",
        json: body,
      }).then(cb.bind(null, null), pipe(get("response.body"), JSON.parse, cb)),
    );
  };
};

export const OaiStreamingClientFactory = (apiKey) => {
  return function (path, body, verb = DEFAULT_VERB) {
    const responseStream = got.stream({
      method: verb,
      url: [API_URL_BASE, path].join("/"),
      headers: {
        Authorization: ["Bearer", apiKey].join(" "),
      },
      json: body,
    });

    return kefir
      .fromEvents(responseStream, "response")
      .merge(
        kefir
          .fromEvents(responseStream, "error")
          .map(get("response.body"))
          .flatMap(kefir.constantError),
      )
      .take(1)
      .flatMap((response) => {
        const contentType = get("headers.content-type", response);
        if (/^text\/event-stream/.test(contentType)) {
          const lineStream = pipeline(
            responseStream,
            split(/\r?\n/, null, { trailing: true }),
            noop,
          );

          return kefir
            .merge([
              kefir.fromEvents(lineStream, "data"),
              kefir
                .fromEvents(lineStream, "error")
                .flatMap(kefir.constantError),
            ])
            .takeUntilBy(kefir.fromEvents(responseStream, "end").take(1))
            .bufferWhile((line) => line.length > 0)
            .flatten((buffer) => {
              const chunkMap = buffer.reduce((map, line) => {
                const { command, value } =
                  line.match(/^(?<command>[^:]+):\s(?<value>.+)/)?.groups ?? {};
                if (command) map.set(command, (map.get(value) ?? "") + value);
                return map;
              }, new Map());

              return chunkMap.has("data")
                ? [
                    Object.fromEntries(
                      [...chunkMap.entries()].map(([key, value]) => [
                        key,
                        (key === "data" ? JSON.parse : identity)(value),
                      ]),
                    ),
                  ]
                : [];
            });
        } else {
          return kefir.fromNodeCallback((cb) =>
            pipeline(
              responseStream,
              concatStream(pipe(toString, JSON.parse, cb.bind(null, null))),
              cb,
            ),
          );
        }
      });
  };
};