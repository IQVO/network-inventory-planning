import { useCallback, useEffect, useRef, useState } from "react";
import { toApiError } from "../api";
import type { ApiError } from "../api";

export type RequestState<T> =
  | { status: "idle" }
  | { status: "loading" }
  | { status: "success"; data: T }
  | { status: "error"; error: ApiError };

/**
 * Runs `load` whenever `key` changes (and on `reload()`); `null` means "not
 * asked yet" (idle). Unlike ui-kit's useFetch this keeps the RFC 7807 body of
 * a failed response, so a screen can show the problem's title AND detail.
 * A response that arrives after the key changed (or after unmount) is dropped.
 *
 * `key` must identify everything `load` depends on: the closure itself is
 * read through a ref so callers can pass an inline arrow.
 */
export function useRequest<T>(
  load: (() => Promise<T>) | null,
  key: string,
): RequestState<T> & { reload: () => void } {
  const loadRef = useRef(load);
  useEffect(() => {
    loadRef.current = load;
  });
  const active = load !== null;
  const [nonce, setNonce] = useState(0);
  const [state, setState] = useState<RequestState<T>>(active ? { status: "loading" } : { status: "idle" });

  useEffect(() => {
    const fn = loadRef.current;
    if (!fn) {
      setState({ status: "idle" });
      return;
    }
    let stale = false;
    setState({ status: "loading" });
    fn().then(
      (data) => {
        if (!stale) setState({ status: "success", data });
      },
      (err: unknown) => {
        if (!stale) setState({ status: "error", error: toApiError(err) });
      },
    );
    return () => {
      stale = true;
    };
  }, [key, nonce, active]);

  const reload = useCallback(() => setNonce((n) => n + 1), []);
  return { ...state, reload };
}
