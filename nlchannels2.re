exception Closed_channel;
exception Buffer_underrun;
exception Command_failure(Unix.process_status);

let () =
  Printexc.register_printer(e =>
    switch (e) {
    | Command_failure(ps) =>
      let ps_str =
        switch (ps) {
        | Unix.WEXITED(n) => "WEXITED " ++ string_of_int(n)
        | Unix.WSIGNALED(n) => "WSIGNALED " ++ string_of_int(n)
        | Unix.WSTOPPED(n) => "WSTOPPED " ++ string_of_int(n)
        };

      Some("Netchannels.Command_failure(" ++ ps_str ++ ")");
    | _ => None
    }
  );

class type rec_in_channel = {
  pub input: (string, int, int) => int;
  pub close_in: unit => unit;
};

class type raw_in_channel =
  {
    inherit rec_in_channel;
    pub pos_in: int;
  }; /* number of read characters */

type input_result = [ | `Data(int) | `Separator(string)];

class type enhanced_raw_in_channel = {
  inherit raw_in_channel;
  pri enhanced_input_line: unit => string;
  pri enhanced_input: (string, int, int) => input_result;
};

class type rec_out_channel = {
  pub output: (string, int, int) => int;
  pub close_out: unit => unit;
  pub flush: unit => unit;
};

class type raw_out_channel =
  {
    inherit rec_out_channel;
    pub pos_out: int;
  }; /* number of written characters */

class type raw_io_channel = {
  inherit raw_in_channel;
  inherit raw_out_channel;
};

class type compl_in_channel =
  {
    pub really_input: (string, int, int) => unit;
    pub input_char: unit => char;
    pub input_line: unit => string;
    pub input_byte: unit => int;
  };
  /* Classic operations: */

class type in_obj_channel = {
  inherit raw_in_channel;
  inherit compl_in_channel;
};

class type compl_out_channel =
  {
    pub really_output: (string, int, int) => unit;
    pub output_char: char => unit;
    pub output_string: string => unit;
    pub output_byte: int => unit;
    pub output_buffer: Buffer.t => unit;
    pub output_channel: (~len: int=?, in_obj_channel) => unit;
  };
  /* ~len: optionally limit the number of bytes */
  /* Classic operations: */

class type out_obj_channel = {
  inherit raw_out_channel;
  inherit compl_out_channel;
};

class type io_obj_channel = {
  inherit in_obj_channel;
  inherit out_obj_channel;
};

class type trans_out_obj_channel = {
  inherit out_obj_channel;
  pub commit_work: unit => unit;
  pub rollback_work: unit => unit;
};

/* error_behavior: currently not used. This was a proposal to control
 * error handling, but it is not clear whether it is really
 * useful or not.
 * I do not delete these types because they remind us of this
 * possibility. Maybe we find an outstanding example for them, and
 * want to have them back.
 */

type error_behavior = [ | `Close | `Fun(unit => unit) | `None];

type extended_error_behavior = [
  | `Close
  | `Rollback
  | `Fun(unit => unit)
  | `None
];

type close_mode = [ | `Commit | `Rollback];

/* Delegation */

class rec_in_channel_delegation (~close=true, ch: rec_in_channel) = {
  as self;
  pub input = ch#input;
  pub close_in = () =>
    if (close) {
      ch#close_in();
    };
};

class raw_in_channel_delegation (~close=true, ch: raw_in_channel) = {
  as self;
  pub input = ch#input;
  pub close_in = () =>
    if (close) {
      ch#close_in();
    };
  pub pos_in = ch#pos_in;
};

class in_obj_channel_delegation (~close=true, ch: in_obj_channel) = {
  as self;
  pub input = ch#input;
  pub close_in = () =>
    if (close) {
      ch#close_in();
    };
  pub pos_in = ch#pos_in;
  pub really_input = ch#really_input;
  pub input_char = ch#input_char;
  pub input_line = ch#input_line;
  pub input_byte = ch#input_byte;
};

class rec_out_channel_delegation (~close=true, ch: rec_out_channel) = {
  as self;
  pub output = ch#output;
  pub close_out = () =>
    if (close) {
      ch#close_out();
    };
  pub flush = ch#flush;
};

class raw_out_channel_delegation (~close=true, ch: raw_out_channel) = {
  as self;
  pub output = ch#output;
  pub close_out = () =>
    if (close) {
      ch#close_out();
    };
  pub flush = ch#flush;
  pub pos_out = ch#pos_out;
};

class out_obj_channel_delegation (~close=true, ch: out_obj_channel) = {
  as self;
  pub output = ch#output;
  pub close_out = () =>
    if (close) {
      ch#close_out();
    };
  pub flush = ch#flush;
  pub pos_out = ch#pos_out;
  pub really_output = ch#really_output;
  pub output_char = ch#output_char;
  pub output_string = ch#output_string;
  pub output_byte = ch#output_byte;
  pub output_buffer = ch#output_buffer;
  pub output_channel = ch#output_channel;
};

/****************************** input ******************************/

class input_channel (~onclose=() => (), ch /* : in_obj_channel */) = {
  as self;
  val ch = ch;
  val mutable closed = false;
  pri complain_closed = () => raise(Closed_channel);
  pub input = (buf, pos, len) => {
    if (closed) {
      self#complain_closed();
    };
    try(
      {
        if (len == 0) {
          raise(Sys_blocked_io);
        };
        let n = Stdlib.input(ch, buf, pos, len);
        if (n == 0) {
          raise(End_of_file);
        } else {
          n;
        };
      }
    ) {
    | Sys_blocked_io => 0
    };
  };
  pub really_input = (buf, pos, len) => {
    if (closed) {
      self#complain_closed();
    };
    Stdlib.really_input(ch, buf, pos, len);
  };
  pub input_char = () => {
    if (closed) {
      self#complain_closed();
    };
    Stdlib.input_char(ch);
  };
  pub input_line = () => {
    if (closed) {
      self#complain_closed();
    };
    Stdlib.input_line(ch);
  };
  pub input_byte = () => {
    if (closed) {
      self#complain_closed();
    };
    Stdlib.input_byte(ch);
  };
  pub close_in = () =>
    if (!closed) {
      Stdlib.close_in(ch);
      closed = true;
      onclose();
    };
  pub pos_in = {
    if (closed) {
      self#complain_closed();
    };
    Stdlib.pos_in(ch);
  };
};

class input_command (cmd) = {
  let ch = Unix.open_process_in(cmd);
  as self;
  inherit (class input_channel)(ch) as super;
  pub close_in = () =>
    if (!closed) {
      let p = Unix.close_process_in(ch);
      closed = true;
      if (p != Unix.WEXITED(0)) {
        raise(Command_failure(p));
      };
    };
};

class input_string (~pos=0, ~len=?, s) : in_obj_channel = {
  as self;
  val mutable str = s;
  val mutable str_len =
    switch (len) {
    | None => String.length(s)
    | Some(l) => pos + l
    };
  val mutable str_pos = pos;
  val mutable closed = false;
  initializer (
    if (str_pos < 0
        || str_pos > String.length(str)
        || str_len < 0
        || str_len > String.length(s)) {
      invalid_arg("new Netchannels.input_string");
    }
  );
  pri complain_closed = () => raise(Closed_channel);
  pub input = (buf, pos, len) => {
    if (closed) {
      self#complain_closed();
    };
    if (pos < 0 || len < 0 || pos + len > String.length(buf)) {
      invalid_arg("input");
    };

    let n = min(len, str_len - str_pos);
    String.blit(str, str_pos, Bytes.of_string(buf), pos, n);

    str_pos = str_pos + n;

    if (n == 0 && len > 0) {
      raise(End_of_file);
    } else {
      n;
    };
  };
  pub really_input = (buf, pos, len) => {
    if (closed) {
      self#complain_closed();
    };
    if (pos < 0 || len < 0 || pos + len > String.length(buf)) {
      invalid_arg("really_input");
    };

    let n = self#input(buf, pos, len);
    if (n != len) {
      raise(End_of_file);
    };
    ();
  };
  pub input_char = () => {
    if (closed) {
      self#complain_closed();
    };
    if (str_pos >= str_len) {
      raise(End_of_file);
    };
    let c = str.[str_pos];
    str_pos = str_pos + 1;
    c;
  };
  pub input_line = () => {
    if (closed) {
      self#complain_closed();
    };
    try({
      let k = String.index_from(str, str_pos, '\n');
      /* CHECK: Are the different end of line conventions important here? */
      let line = String.sub(str, str_pos, k - str_pos);
      str_pos = k + 1;
      line;
    }) {
    | Not_found =>
      if (str_pos >= str_len) {
        raise(End_of_file);
      };
      /* Implicitly add linefeed at the end of the file: */
      let line = String.sub(str, str_pos, str_len - str_pos);
      str_pos = str_len;
      line;
    };
  };
  pub input_byte = () => Char.code(self#input_char());
  pub close_in = () => {
    str = "";
    closed = true;
  };
  pub pos_in = {
    if (closed) {
      self#complain_closed();
    };
    str_pos;
  };
};

class type nb_in_obj_channel = {
  inherit in_obj_channel;
  pub shutdown: unit => unit;
};

class input_netbuffer (b) : nb_in_obj_channel = {
  as self;
  val mutable b = b;
  val mutable eof = false;
  val mutable closed = false;
  val mutable ch_pos = 0;
  pri complain_closed = () => raise(Closed_channel);
  pub input = (buf, pos, len) => {
    if (closed) {
      self#complain_closed();
    };
    if (pos < 0 || len < 0 || pos + len > String.length(buf)) {
      invalid_arg("input");
    };

    let n = min(len, Nlbuffer.length(b));
    if (n == 0 && len > 0) {
      if (eof) {
        raise(End_of_file);
      } else {
        raise(Buffer_underrun);
      };
    } else {
      Nlbuffer.blit(b, 0, buf, pos, n);
      Nlbuffer.delete(b, 0, n);
      ch_pos = ch_pos + n;
      n;
    };
  };
  pub really_input = (buf, pos, len) => {
    if (closed) {
      self#complain_closed();
    };
    if (pos < 0 || len < 0 || pos + len > String.length(buf)) {
      invalid_arg("really_input");
    };

    let n = self#input(buf, pos, len);
    if (n != len) {
      raise(End_of_file);
    };
    ();
  };
  pub input_char = () => {
    if (closed) {
      self#complain_closed();
    };
    let s = Bytes.create(1);
    switch (self#input(Bytes.to_string(s), 0, 1)) {
    | 1 => Bytes.to_string(s).[0]
    | _ => assert(false)
    };
  };
  pub input_line = () => {
    if (closed) {
      self#complain_closed();
    };
    try({
      let k = Nlbuffer.index_from(b, 0, '\n');
      /* CHECK: Are the different end of line conventions important here? */
      let line = Nlbuffer.sub(b, 0, k);
      Nlbuffer.delete(b, 0, k + 1);
      ch_pos = ch_pos + k + 1;
      line;
    }) {
    | Not_found =>
      if (eof) {
        if (Nlbuffer.length(b) == 0) {
          raise(End_of_file);
        };
        /* Implicitly add linefeed at the end of the file: */
        let line = Nlbuffer.contents(b);
        Nlbuffer.clear(b);
        ch_pos = ch_pos + Nlbuffer.length(b);
        line;
      } else {
        raise(Buffer_underrun);
      }
    };
  };
  pub input_byte = () => Char.code(self#input_char());
  pub close_in = () => closed = true;
  pub pos_in = {
    if (closed) {
      self#complain_closed();
    };
    ch_pos;
  };
  pub shutdown = () => eof = true;
};

let create_input_netbuffer = b => {
  let ch = (new input_netbuffer)(b);
  ((ch :> in_obj_channel), ch#shutdown);
};

let lexbuf_of_in_obj_channel = (objch: in_obj_channel): Lexing.lexbuf => {
  let fill_buffer = (buf:Bytes.t, len) =>
    try({
      let n = objch#input(Bytes.to_string(buf), 0, len);
      if (n == 0) {
        failwith(
          "Netchannels.lexbuf_of_in_obj_channel: No data (non-blocking I/O?)",
        );
      };
      n;
    }) {
    | End_of_file => 0
    };

  Lexing.from_function(fill_buffer);
};

let string_of_in_obj_channel = (objch: in_obj_channel): string => {
  /* There are similarities to copy_channel below. */
  /* The following algorithm uses only up to 2 * N memory, not 3 * N
   * as with the Buffer module.
   */
  let slen = 1024;
  let l = ref([]);
  let k = ref(0);
  try(
    {
      while (true) {
        let s = Bytes.create(slen);
        let n = objch#input(Bytes.to_string(s), 0, slen);
        if (n == 0) {
          failwith(
            "Netchannels.string_of_in_obj_channel: No data (non-blocking I/O?)",
          );
        };
        k := k^ + n;
        if (n < slen) {
          l := [String.sub(Bytes.to_string(s), 0, n), ...l^];
        } else {
          l := [Bytes.to_string(s), ...l^];
        };
      };
      assert(false);
    }
  ) {
  | End_of_file =>
    let s = Bytes.create(k^);
    while (l^ != []) {
      switch (l^) {
      | [u, ...l'] =>
        let n = String.length(u);
        k := k^ - n;
        String.blit(u, 0, s, k^, n);
        l := l';
      | [] => assert(false)
      };
    };
    assert(k^ == 0);
    Bytes.to_string(s);
  };
};

let lines_of_in_obj_channel = ch => {
  let acc = ref([]);
  try(
    {
      while (true) {
        acc := [ch#input_line(), ...acc^];
      };
      assert(false);
    }
  ) {
  | End_of_file => List.rev(acc^)
  };
};

let with_in_obj_channel = (ch, f) =>
  try({
    let result = f(ch);
    try(ch#close_in()) {
    | Closed_channel => ()
    };
    result;
  }) {
  | e =>
    try(ch#close_in()) {
    | Closed_channel => ()
    };
    raise(e);
  };

class virtual augment_raw_in_channel = {
  as self;
  pub virtual input: (string, int, int) => int;
  pub virtual close_in: unit => unit;
  pub virtual pos_in: int;
  pub really_input = (s, pos, len) => {
    let rec read_rest = n =>
      if (n < len) {
        let m = self#input(s, pos + n, len - n);
        if (m == 0) {
          raise(Sys_blocked_io);
        };
        read_rest(n + m);
      } else {
        ();
      };

    read_rest(0);
  };
  pub input_char = () => {
    let s = Bytes.create(1);
    self#really_input(Bytes.to_string(s), 0, 1);
    Bytes.to_string(s).[0];
  };
  pub input_byte = () => {
    let s = Bytes.create(1);
    self#really_input(Bytes.to_string(s), 0, 1);
    Char.code(Bytes.to_string(s).[0]);
  };
  pub input_line = () => {
    let s = Bytes.create(1);
    let b = Buffer.create(80);
    let m = self#input(Bytes.to_string(s), 0, 1);
    if (m == 0) {
      raise(Sys_blocked_io);
    };
    while (Bytes.to_string(s).[0] != '\n') {
      Buffer.add_char(b, Bytes.to_string(s).[0]);
      try({
        let m = self#input(Bytes.to_string(s), 0, 1);
        if (m == 0) {
          raise(Sys_blocked_io);
        };
      }) {
      | End_of_file => (s).[0] = '\n'
      };
    };
    Buffer.contents(b);
  };
};

class lift_raw_in_channel (r) = {
  as self;
  inherit class augment_raw_in_channel;
  pub input = (s, p, l) => r#input(s, p, l);
  pub close_in = () => r#close_in();
  pub pos_in = r#pos_in;
};

class lift_rec_in_channel (~start_pos_in=0, r: rec_in_channel) = {
  as self;
  inherit class augment_raw_in_channel;
  val mutable closed = false;
  val mutable pos_in = start_pos_in;
  pub input = (s, p, l) => {
    if (closed) {
      raise(Closed_channel);
    };
    let n = r#input(s, p, l);
    pos_in = pos_in + n;
    n;
  };
  pub close_in = () =>
    if (!closed) {
      closed = true;
      r#close_in();
    };
  pub pos_in = {
    if (closed) {
      raise(Closed_channel);
    };
    pos_in;
  };
};

type eol_status =
  | EOL_not_found
  | EOL_partially_found(int) /* Position */
  | EOL_found(int, int); /* Position, length */

exception Pass_through;

class buffered_raw_in_channel
      (
        ~eol=["\n"],
        ~buffer_size=4096,
        ~pass_through=max_int,
        ch: raw_in_channel,
      )
      : enhanced_raw_in_channel = {
  as self;
  val out = ch;
  val bufsize = buffer_size;
  val buf = Bytes.create(buffer_size);
  val mutable bufpos = 0;
  val mutable buflen = 0;
  val mutable eof = false;
  val mutable closed = false;
  initializer {
    if (List.exists(s => s == "", eol)) {
      invalid_arg("Netchannels.buffered_raw_in_channel");
    };
    if (List.exists(s => String.length(s) > buffer_size, eol)) {
      invalid_arg("Netchannels.buffered_raw_in_channel");
    };
  };
  pub input = (s, pos, len) => {
    if (closed) {
      raise(Closed_channel);
    };
    try(
      if (len > 0) {
        if (bufpos == buflen) {
          if (len >= pass_through) {
            raise(Pass_through);
          } else {
            self#refill();
          };
        };
        let n = min(len, buflen - bufpos);
        String.blit(Bytes.to_string(buf), bufpos, s, pos, n);
        bufpos = bufpos + n;
        n;
      } else {
        0;
      }
    ) {
    | Pass_through => ch#input(s, pos, len)
    };
  };
  pri refill = () => {
    let d = bufpos;
    if (d > 0 && d < buflen) {
      String.blit(buf, d, buf, 0, buflen - d);
    };
    bufpos = 0;
    buflen = buflen - d;
    try(
      {
        assert(bufsize > buflen); /* otherwise problems... */
        let n = ch#input(buf, buflen, bufsize - buflen); /* or End_of_file */
        if (n == 0) {
          raise(Sys_blocked_io);
        };
        buflen = buflen + n;
      }
    ) {
    | End_of_file as exn =>
      eof = true;
      raise(exn);
    };
  };
  pub close_in = () =>
    if (!closed) {
      ch#close_in();
      closed = true;
    };
  pub pos_in = ch#pos_in - (buflen - bufpos);
  pri find_eol = () => {
    /* Try all strings from [eol] in turn. For every string we may
     * have three results:
     * - Not found
     * - Partially found
     * - Found
     * The eol delimiter is only found if there are no partial
     * results, and at least one positive result. The longest
     * string is taken.
     */

    let find_this_eol = eol => {
      /* Try to find the eol string [eol] in [buf] starting at
       * [bufpos] up to [buflen]. Return [eol_status].
       */
      let eol0 = eol.[0];
      try({
        let k = String.index_from(buf, bufpos, eol0); /* or Not_found */
        if (k >= buflen) {
          raise(Not_found);
        };
        let k' = min(buflen, k + String.length(eol));
        let s = String.sub(buf, k, k' - k);
        if (s == eol) {
          [@implicit_arity] EOL_found(k, String.length(eol));
        } else if (!eof && String.sub(eol, 0, String.length(s)) == s) {
          EOL_partially_found(k);
        } else {
          EOL_not_found;
        };
      }) {
      | Not_found => EOL_not_found
      };
    };

    let rec find_best_eol = (best, eol_result) =>
      switch (eol_result) {
      | [EOL_not_found, ...eol_result'] => find_best_eol(best, eol_result')
      | [EOL_partially_found(pos) as r, ...eol_result'] =>
        switch (best) {
        | EOL_partially_found(pos') =>
          if (pos < pos') {
            find_best_eol(r, eol_result');
          } else {
            find_best_eol(best, eol_result');
          }
        | _ => find_best_eol(r, eol_result')
        }
      | [[@implicit_arity] EOL_found(pos, len) as r, ...eol_result'] =>
        switch (best) {
        | [@implicit_arity] EOL_found(pos', len') =>
          if (pos < pos' || pos == pos' && len > len') {
            find_best_eol(r, eol_result');
          } else {
            find_best_eol(best, eol_result');
          }
        | EOL_partially_found(_) => find_best_eol(best, eol_result')
        | EOL_not_found => find_best_eol(r, eol_result')
        }
      | [] => best
      };

    let eol_results = List.map(find_this_eol, eol);
    find_best_eol(EOL_not_found, eol_results);
  };
  pri enhanced_input = (s, pos, len): input_result => {
    if (closed) {
      raise(Closed_channel);
    };
    if (len > 0) {
      if (bufpos == buflen) {
        self#refill(); /* may raise End_of_file */
      };
      let result = ref(None);
      while (result^ == None) {
        let best = self#find_eol();
        switch (best) {
        | EOL_not_found =>
          let n = min(len, buflen - bufpos);
          String.blit(buf, bufpos, s, pos, n);
          bufpos = bufpos + n;
          result := Some(`Data(n));
        | [@implicit_arity] EOL_found(p, l) =>
          if (p == bufpos) {
            bufpos = bufpos + l;
            result := Some(`Separator(String.sub(buf, p, l)));
          } else {
            let n = min(len, p - bufpos);
            String.blit(buf, bufpos, s, pos, n);
            bufpos = bufpos + n;
            result := Some(`Data(n));
          }
        | EOL_partially_found(p) =>
          if (p == bufpos) {
            try(self#refill()) {
            | End_of_file => ()
            /* ... and continue! */
            };
          } else {
            let n = min(len, p - bufpos);
            String.blit(buf, bufpos, s, pos, n);
            bufpos = bufpos + n;
            result := Some(`Data(n));
          }
        };
      };
      switch (result^) {
      | None => assert(false)
      | Some(r) => r
      };
    } else {
      `Data(0);
    };
  };
  pri enhanced_input_line = () => {
    if (closed) {
      raise(Closed_channel);
    };
    let b = Buffer.create(80);
    let eol_found = ref(false);
    if (bufpos == buflen) {
      self#refill(); /* may raise End_of_file */
    };
    while (! eol_found^) {
      let best = self#find_eol();
      try(
        switch (best) {
        | EOL_not_found =>
          Buffer.add_substring(b, buf, bufpos, buflen - bufpos);
          bufpos = buflen;
          self#refill(); /* may raise End_of_file */
        | EOL_partially_found(pos) =>
          Buffer.add_substring(b, buf, bufpos, pos - bufpos);
          bufpos = pos;
          self#refill(); /* may raise End_of_file */
        | [@implicit_arity] EOL_found(pos, len) =>
          Buffer.add_substring(b, buf, bufpos, pos - bufpos);
          bufpos = pos + len;
          eol_found := true;
        }
      ) {
      | End_of_file =>
        bufpos = 0;
        buflen = 0;
        eof = true;
        eol_found := true;
      };
    };
    Buffer.contents(b);
  };
};

class lift_raw_in_channel_buf (~eol=?, ~buffer_size=?, ~pass_through=?, r) = {
  as self;
  inherit
    (class buffered_raw_in_channel)(~eol?, ~buffer_size?, ~pass_through?, r);
  inherit class augment_raw_in_channel;
  pub input_line = () => self#enhanced_input_line();
};

type lift_in_arg = [ | `Rec(rec_in_channel) | `Raw(raw_in_channel)];

let lift_in =
    (
      ~eol=["\n"],
      ~buffered=true,
      ~buffer_size=?,
      ~pass_through=?,
      x: lift_in_arg,
    ) =>
  switch (x) {
  | `Rec(r) when !buffered =>
    if (eol != ["\n"]) {
      invalid_arg("Netchannels.lift_in");
    };
    (new lift_rec_in_channel)(r);
  | `Rec(r) =>
    let r' = (new lift_rec_in_channel)(r);
    (new lift_raw_in_channel_buf)(
      ~eol,
      ~buffer_size?,
      ~pass_through?,
      (r' :> raw_in_channel),
    );
  | `Raw(r) when !buffered =>
    if (eol != ["\n"]) {
      invalid_arg("Netchannels.lift_in");
    };
    (new lift_raw_in_channel)(r);
  | `Raw(r) =>
    (new lift_raw_in_channel_buf)(~eol, ~buffer_size?, ~pass_through?, r)
  };

/****************************** output ******************************/

exception No_end_of_file;

let copy_channel =
    (
      ~buf=Bytes.create(1024),
      ~len=?,
      src_ch: in_obj_channel,
      dest_ch: out_obj_channel,
    ) => {
  /* Copies contents from src_ch to dest_ch. Returns [true] if at EOF.
   */
  let slen = String.length(buf);
  let k = ref(0);
  try(
    {
      while (true) {
        let m =
          min(
            slen,
            switch (len) {
            | Some(x) => x - k^
            | None => max_int
            },
          );
        if (m <= 0) {
          raise(No_end_of_file);
        };
        let n = src_ch#input(buf, 0, m);
        if (n == 0) {
          raise(Sys_blocked_io);
        };
        dest_ch#really_output(buf, 0, n);
        k := k^ + n;
      };
      assert(false);
    }
  ) {
  | End_of_file => true
  | No_end_of_file => false
  };
};

class output_channel (~onclose=() => (), ch) = {
  /* : out_obj_channel */
  let errflag = ref(false);
  let monitored = (f, arg) =>
    try({
      let r = f(arg);
      errflag := false;
      r;
    }) {
    | error =>
      errflag := true;
      raise(error);
    };
  as self;
  val ch = ch;
  val onclose = onclose;
  val mutable closed = false;
  pri complain_closed = () => raise(Closed_channel);
  pub output = (buf, pos, len) => {
    if (closed) {
      self#complain_closed();
    };
    /* Stdlib.output does not support non-blocking I/O directly.
     * Work around it:
     */
    let p0 = Stdlib.pos_out(ch);
    try(
      {
        Stdlib.output(ch, buf, pos, len);
        errflag := false;
        len;
      }
    ) {
    | Sys_blocked_io =>
      let p1 = Stdlib.pos_out(ch);
      errflag := false;
      p1 - p0;
    | error =>
      errflag := true;
      raise(error);
    };
  };
  pub really_output = (buf, pos, len) => {
    if (closed) {
      self#complain_closed();
    };
    monitored(Stdlib.output(ch, buf, pos), len);
  };
  pub output_char = c => {
    if (closed) {
      self#complain_closed();
    };
    monitored(Stdlib.output_char(ch), c);
  };
  pub output_string = s => {
    if (closed) {
      self#complain_closed();
    };
    monitored(Stdlib.output_string(ch), s);
  };
  pub output_byte = b => {
    if (closed) {
      self#complain_closed();
    };
    monitored(Stdlib.output_byte(ch), b);
  };
  pub output_buffer = b => {
    if (closed) {
      self#complain_closed();
    };
    monitored(Buffer.output_buffer(ch), b);
  };
  pub output_channel = (~len=?, ch) => {
    if (closed) {
      self#complain_closed();
    };
    ignore(
      monitored(
        copy_channel(~len?, ch),
        (self: #out_obj_channel :> out_obj_channel),
      ),
    );
  };
  pub flush = () => {
    if (closed) {
      self#complain_closed();
    };
    monitored(Stdlib.flush, ch);
  };
  pub close_out = () =>
    if (!closed) {
      try(Stdlib.close_out_noerr(ch)) {
      | _ => assert(false)
      };
      closed = true;
      onclose();
    };
  pub pos_out = {
    if (closed) {
      self#complain_closed();
    };
    Stdlib.pos_out(ch);
  };
};

class output_command (~onclose=?, cmd) = {
  let ch = Unix.open_process_out(cmd);
  as self;
  inherit (class output_channel)(~onclose?, ch) as super;
  pub close_out = () =>
    if (!closed) {
      let p = Unix.close_process_out(ch);
      closed = true;
      onclose();
      if (p != Unix.WEXITED(0)) {
        raise(Command_failure(p));
      }; /* Keep this */
    };
};

class output_buffer (~onclose=() => (), buffer) : out_obj_channel = {
  as self;
  val buffer = buffer;
  val onclose = onclose;
  val mutable closed = false;
  pri complain_closed = () => raise(Closed_channel);
  pub output = (buf, pos, len) => {
    if (closed) {
      self#complain_closed();
    };
    Buffer.add_substring(buffer, buf, pos, len);
    len;
  };
  pub really_output = (buf, pos, len) => {
    if (closed) {
      self#complain_closed();
    };
    Buffer.add_substring(buffer, buf, pos, len);
  };
  pub output_char = c => {
    if (closed) {
      self#complain_closed();
    };
    Buffer.add_char(buffer, c);
  };
  pub output_string = s => {
    if (closed) {
      self#complain_closed();
    };
    Buffer.add_string(buffer, s);
  };
  pub output_byte = b => {
    if (closed) {
      self#complain_closed();
    };
    Buffer.add_char(buffer, Char.chr(b));
  };
  pub output_buffer = b => {
    if (closed) {
      self#complain_closed();
    };
    Buffer.add_buffer(buffer, b);
  };
  pub output_channel = (~len=?, ch) => {
    if (closed) {
      self#complain_closed();
    };
    ignore(
      copy_channel(~len?, ch, (self: #out_obj_channel :> out_obj_channel)),
    );
  };
  pub flush = () => {
    if (closed) {
      self#complain_closed();
    };
    ();
  };
  pub close_out = () =>
    if (!closed) {
      closed = true;
      onclose();
    };
  pub pos_out = {
    if (closed) {
      self#complain_closed();
    };
    Buffer.length(buffer);
  };
};

class output_netbuffer (~onclose=() => (), buffer) : out_obj_channel = {
  as self;
  val buffer = buffer;
  val onclose = onclose;
  val mutable closed = false;
  val mutable ch_pos = 0;
  pri complain_closed = () => raise(Closed_channel);
  pub output = (buf, pos, len) => {
    if (closed) {
      self#complain_closed();
    };
    Nlbuffer.add_sub_string(buffer, buf, pos, len);
    ch_pos = ch_pos + len;
    len;
  };
  pub really_output = (buf, pos, len) => {
    if (closed) {
      self#complain_closed();
    };
    Nlbuffer.add_sub_string(buffer, buf, pos, len);
    ch_pos = ch_pos + len;
  };
  pub output_char = c => {
    if (closed) {
      self#complain_closed();
    };
    Nlbuffer.add_string(buffer, String.make(1, c));
    ch_pos = ch_pos + 1;
  };
  pub output_string = s => {
    if (closed) {
      self#complain_closed();
    };
    Nlbuffer.add_string(buffer, s);
    ch_pos = ch_pos + String.length(s);
  };
  pub output_byte = b => {
    if (closed) {
      self#complain_closed();
    };
    Nlbuffer.add_string(buffer, String.make(1, Char.chr(b)));
    ch_pos = ch_pos + 1;
  };
  pub output_buffer = b => {
    if (closed) {
      self#complain_closed();
    };
    Nlbuffer.add_string(buffer, Buffer.contents(b));
    ch_pos = ch_pos + Buffer.length(b);
  };
  pub output_channel = (~len=?, ch) => {
    if (closed) {
      self#complain_closed();
    };
    ignore(
      copy_channel(~len?, ch, (self: #out_obj_channel :> out_obj_channel)),
    );
  };
  pub flush = () => {
    if (closed) {
      self#complain_closed();
    };
    ();
  };
  pub close_out = () =>
    if (!closed) {
      closed = true;
      onclose();
    };
  pub pos_out = {
    if (closed) {
      self#complain_closed();
    };
    ch_pos;
  };
  /* We cannot return Nlbuffer.length b as [pos_out] (like in the class
   * [output_buffer]) because the user of this class is allowed to delete
   * data from the netbuffer. So we manually count how many bytes are
   * ever appended to the netbuffer.
   * This behavior is especially needed by [pipe_channel] below.
   */
};

class output_null (~onclose=() => (), ()) : out_obj_channel = {
  as self;
  val mutable closed = false;
  val mutable pos = 0;
  pri complain_closed = () => raise(Closed_channel);
  pub output = (s, start, len) => {
    if (closed) {
      self#complain_closed();
    };
    pos = pos + len;
    len;
  };
  pub really_output = (s, start, len) => {
    if (closed) {
      self#complain_closed();
    };
    pos = pos + len;
  };
  pub output_char = _ => {
    if (closed) {
      self#complain_closed();
    };
    pos = pos + 1;
  };
  pub output_string = s => {
    if (closed) {
      self#complain_closed();
    };
    pos = pos + String.length(s);
  };
  pub output_byte = _ => {
    if (closed) {
      self#complain_closed();
    };
    pos = pos + 1;
  };
  pub output_buffer = b => {
    if (closed) {
      self#complain_closed();
    };
    pos = pos + Buffer.length(b);
  };
  pub output_channel = (~len=?, ch) => {
    if (closed) {
      self#complain_closed();
    };
    ignore(
      copy_channel(~len?, ch, (self: #out_obj_channel :> out_obj_channel)),
    );
  };
  pub flush = () =>
    if (closed) {
      self#complain_closed();
    };
  pub close_out = () => closed = true;
  pub pos_out = {
    if (closed) {
      self#complain_closed();
    };
    pos;
  };
};

let with_out_obj_channel = (ch, f) =>
  try({
    let result = f(ch);
    /* we _have_ to flush here because close_out often does no longer
         report exceptions
       */
    try(ch#flush()) {
    | Closed_channel => ()
    };
    try(ch#close_out()) {
    | Closed_channel => ()
    };
    result;
  }) {
  | e =>
    try(ch#close_out()) {
    | Closed_channel => ()
    };
    raise(e);
  };

class virtual augment_raw_out_channel = {
  as self;
  pub virtual output: (string, int, int) => int;
  pub virtual close_out: unit => unit;
  pub virtual flush: unit => unit;
  pub virtual pos_out: int;
  pub really_output = (s, pos, len) => {
    let rec print_rest = n =>
      if (n < len) {
        let m = self#output(s, pos + n, len - n);
        if (m == 0) {
          raise(Sys_blocked_io);
        };
        print_rest(n + m);
      } else {
        ();
      };

    print_rest(0);
  };
  pub output_char = c => ignore(self#output(String.make(1, c), 0, 1));
  pub output_byte = n =>
    ignore(self#output(String.make(1, Char.chr(n)), 0, 1));
  pub output_string = s => self#really_output(s, 0, String.length(s));
  pub output_buffer = b => self#output_string(Buffer.contents(b));
  pub output_channel = (~len=?, ch) =>
    ignore(
      copy_channel(~len?, ch, (self: #out_obj_channel :> out_obj_channel)),
    );
};

class lift_raw_out_channel (r: raw_out_channel) = {
  as self;
  inherit class augment_raw_out_channel;
  pub output = (s, p, l) => r#output(s, p, l);
  pub flush = () => r#flush();
  pub close_out = () => r#close_out();
  pub pos_out = r#pos_out;
};

class lift_rec_out_channel (~start_pos_out=0, r: rec_out_channel) = {
  as self;
  inherit class augment_raw_out_channel;
  val mutable closed = false;
  val mutable pos_out = start_pos_out;
  pub output = (s, p, l) => {
    if (closed) {
      raise(Closed_channel);
    };
    let n = r#output(s, p, l);
    pos_out = pos_out + n;
    n;
  };
  pub flush = () => {
    if (closed) {
      raise(Closed_channel);
    };
    r#flush();
  };
  pub close_out = () =>
    if (!closed) {
      closed = true;
      r#close_out();
    };
  pub pos_out = {
    if (closed) {
      raise(Closed_channel);
    };
    pos_out;
  };
};

class buffered_raw_out_channel
      (~buffer_size=4096, ~pass_through=max_int, ch: raw_out_channel)
      : raw_out_channel = {
  as self;
  val out = ch;
  val bufsize = buffer_size;
  val buf = Bytes.create(buffer_size);
  val mutable bufpos = 0;
  val mutable closed = false;
  pub output = (s, pos, len) => {
    if (closed) {
      raise(Closed_channel);
    };
    if (bufpos == 0 && len >= pass_through) {
      ch#output(s, pos, len);
    } else {
      let n = min(len, bufsize - bufpos);
      String.blit(s, pos, buf, bufpos, n);
      bufpos = bufpos + n;
      if (bufpos == bufsize) {
        self#flush();
      };
      n;
    };
  };
  pub flush = () => {
    let k = ref(0);
    while (k^ < bufpos) {
      k := k^ + ch#output(Bytes.to_string(buf), k^, bufpos - k^);
    };
    bufpos = 0;
    ch#flush();
  };
  pub close_out = () =>
    if (!closed) {
      try(self#flush()) {
      | error =>
        let bt = Printexc.get_backtrace();
        Printf.eprintf(
          "Netchannels.buffered_raw_out_channel: Suppressed error in close_out: %s - backtrace: %s",
          Printexc.to_string(error),
          bt,
        );
      };
      ch#close_out();
      closed = true;
    };
  pub pos_out = ch#pos_out + bufpos;
};

type lift_out_arg = [ | `Rec(rec_out_channel) | `Raw(raw_out_channel)];

let lift_out =
    (~buffered=true, ~buffer_size=?, ~pass_through=?, x: lift_out_arg) =>
  switch (x) {
  | `Rec(r) when !buffered => (new lift_rec_out_channel)(r)
  | `Rec(r) =>
    let r' = (new lift_rec_out_channel)(r);
    let r'' =
      (new buffered_raw_out_channel)(
        ~buffer_size?,
        ~pass_through?,
        (r' :> raw_out_channel),
      );
    (new lift_raw_out_channel)(r'');
  | `Raw(r) when !buffered => (new lift_raw_out_channel)(r)
  | `Raw(r) =>
    let r' = (new buffered_raw_out_channel)(~buffer_size?, ~pass_through?, r);
    (new lift_raw_out_channel)(r');
  };

/************************** transactional *****************************/

class buffered_trans_channel
      (~close_mode=(`Commit: close_mode), ch: out_obj_channel)
      : trans_out_obj_channel = {
  let closed = ref(false);
  let transbuf = ref(Buffer.create(50));
  let trans = ref((new output_buffer)(transbuf^));
  let reset = () => {
    transbuf := Buffer.create(50);
    trans := (new output_buffer)(transbuf^);
  };
  as self;
  val out = ch;
  val close_mode = close_mode;
  pub output = (trans^)#output;
  pub really_output = (trans^)#really_output;
  pub output_char = (trans^)#output_char;
  pub output_string = (trans^)#output_string;
  pub output_byte = (trans^)#output_byte;
  pub output_buffer = (trans^)#output_buffer;
  pub output_channel = (trans^)#output_channel;
  pub flush = (trans^)#flush;
  pub close_out = () =>
    if (! closed^) {
      try(
        switch (close_mode) {
        | `Commit => self#commit_work()
        | `Rollback => self#rollback_work()
        }
      ) {
      | error =>
        let bt = Printexc.get_backtrace();
        Printf.eprintf(
          "Netchannels.buffered_trans_channel: Suppressed error in close_out: %s - backtrace: %s",
          Printexc.to_string(error),
          bt,
        );
      };
      (trans^)#close_out();
      out#close_out();
      closed := true;
    };
  pub pos_out = out#pos_out + (trans^)#pos_out;
  pub commit_work = () =>
    /* in any way avoid that the contents of transbuf are printed twice */
    try({
      let b = transbuf^;
      reset();
      out#output_buffer(b);
      out#flush();
    }) {
    | err =>
      self#rollback_work(); /* reset anyway */
      raise(err);
    };
  pub rollback_work = () => reset();
};

let id_conv = (incoming, incoming_eof, outgoing) => {
  /* Copies everything from [incoming] to [outgoing] */
  let len = Nlbuffer.length(incoming);
  ignore(
    Nlbuffer.add_inplace(
      ~len,
      outgoing,
      (s_outgoing, pos, len') => {
        assert(len == len');
        Nlbuffer.blit(incoming, 0, s_outgoing, pos, len');
        Nlbuffer.clear(incoming);
        len';
      },
    ),
  );
};

let call_input = (refill, f, arg) =>
  /* Try to satisfy the request: */
  try(f(arg)) {
  | Buffer_underrun =>
    /* Not enough data in the outgoing buffer. */
    refill();
    f(arg);
  };

class pipe (~conv=id_conv, ~buffer_size=1024, ()) : io_obj_channel = {
  let _incoming = Nlbuffer.create(buffer_size);
  let _outgoing = Nlbuffer.create(buffer_size);
  as self;
  /* The properties as "incoming buffer" [output_super] are simply inherited
   * from [output_netbuffer]. The "outgoing buffer" [input_super] invocations
   * are delegated to [input_netbuffer]. Inheritance does not work because
   * there is no way to make the public method [shutdown] private again.
   */
  inherit (class output_netbuffer)(_incoming) as output_super;
  val conv = conv;
  val incoming = _incoming;
  val outgoing = _outgoing;
  val input_super = (new input_netbuffer)(_outgoing);
  val mutable incoming_eof = false;
  val mutable pos_in = 0;
  /* We must count positions ourselves. Can't use input_super#pos_in
   * because conv may manipulate the buffer.
   */
  val mutable output_closed = false;
  /* Input methods: */
  pri refill = () => {
    conv(incoming, incoming_eof, outgoing);
    if (incoming_eof) {
      input_super#shutdown();
    };
  };
  pub input = (str, pos, len) => {
    let n = call_input(self#refill, input_super#input(str, pos), len);
    pos_in = pos_in + n;
    n;
  };
  pub input_line = () => {
    let p = input_super#pos_in;
    let line = call_input(self#refill, input_super#input_line, ());
    let p' = input_super#pos_in;
    pos_in = pos_in + (p' - p);
    line;
  };
  pub really_input = (str, pos, len) => {
    call_input(self#refill, input_super#really_input(str, pos), len);
    pos_in = pos_in + len;
  };
  pub input_char = () => {
    let c = call_input(self#refill, input_super#input_char, ());
    pos_in = pos_in + 1;
    c;
  };
  pub input_byte = () => {
    let b = call_input(self#refill, input_super#input_byte, ());
    pos_in = pos_in + 1;
    b;
  };
  pub close_in = () => {
    /* [close_in] implies [close_out]: */
    if (!output_closed) {
      output_super#close_out();
      output_closed = true;
    };
    input_super#close_in();
  };
  pub pos_in = pos_in;
  /* [close_out] also shuts down the input side of the pipe. */
  pub close_out = () => {
    if (!output_closed) {
      output_super#close_out();
      output_closed = true;
    };
    incoming_eof = true;
  };
};

class output_filter
      (p: io_obj_channel, out: out_obj_channel)
      : out_obj_channel = {
  as self;
  val p = p;
  val mutable p_closed = false; /* output side of p is closed */
  val out = out;
  val buf = Bytes.create(1024); /* for copy_channel */
  pub output = (s, pos, len) => {
    if (p_closed) {
      raise(Closed_channel);
    };
    let n = p#output(s, pos, len);
    self#transfer();
    n;
  };
  pub really_output = (s, pos, len) => {
    if (p_closed) {
      raise(Closed_channel);
    };
    p#really_output(s, pos, len);
    self#transfer();
  };
  pub output_char = c => {
    if (p_closed) {
      raise(Closed_channel);
    };
    p#output_char(c);
    self#transfer();
  };
  pub output_string = s => {
    if (p_closed) {
      raise(Closed_channel);
    };
    p#output_string(s);
    self#transfer();
  };
  pub output_byte = b => {
    if (p_closed) {
      raise(Closed_channel);
    };
    p#output_byte(b);
    self#transfer();
  };
  pub output_buffer = b => {
    if (p_closed) {
      raise(Closed_channel);
    };
    p#output_buffer(b);
    self#transfer();
  };
  pub output_channel = (~len=?, ch) => {
    /* To avoid large intermediate buffers, the channel is copied
     * chunk by chunk
     */
    if (p_closed) {
      raise(Closed_channel);
    };
    let len_to_do =
      ref(
        switch (len) {
        | None => (-1)
        | Some(l) => max(0, l)
        },
      );
    let buf = buf;
    while (len_to_do^ != 0) {
      let n =
        if (len_to_do^ < 0) {
          1024;
        } else {
          min(len_to_do^, 1024);
        };
      if (copy_channel(~buf, ~len=n, ch, (p :> out_obj_channel))) {
        /* EOF */
        len_to_do := 0;
      } else if (len_to_do^ >= 0) {
        len_to_do := len_to_do^ - n;
        assert(len_to_do^ >= 0);
      };
      self#transfer();
    };
  };
  pub flush = () => {
    p#flush();
    self#transfer();
    out#flush();
  };
  pub close_out = () =>
    if (!p_closed) {
      p#close_out();
      p_closed = true;
      try(self#transfer()) {
      | error =>
        /* We report the error. However, we prevent that another,
           immediately following [close_out] reports the same
           error again. This is done by setting p_closed.
               */
        raise(error)
      };
    };
  pub pos_out = p#pos_out;
  pri transfer = () =>
    /* Copy as much as possible from [p] to [out] */
    /* Call [copy_channel] directly (and not the method [output_channel])
     * because we can pass the copy buffer ~buf
     */
    try(
      {
        ignore(copy_channel(~buf, (p :> in_obj_channel), out));
        out#flush();
      }
    ) {
    | Buffer_underrun => ()
    };
};

let rec filter_input = (refill, f, arg) =>
  /* Try to satisfy the request: */
  try(f(arg)) {
  | Buffer_underrun =>
    /* Not enough data in the outgoing buffer. */
    refill();
    filter_input(refill, f, arg);
  };

class input_filter (inp: in_obj_channel, p: io_obj_channel) : in_obj_channel = {
  as self;
  val inp = inp;
  val p = p;
  val buf = Bytes.create(1024); /* for copy_channel */
  pri refill = () => {
    /* Copy some data from [inp] to [p] */
    /* Call [copy_channel] directly (and not the method [output_channel])
     * because we can pass the copy buffer ~buf
     */
    let eof =
      copy_channel(
        ~len=String.length(Bytes.to_string(buf)),
        ~buf,
        inp,
        (p :> out_obj_channel),
      );
    if (eof) {
      p#close_out();
    };
  };
  pub input = (str, pos) => filter_input(self#refill, p#input(str, pos));
  pub input_line = filter_input(self#refill, p#input_line);
  pub really_input = (str, pos) =>
    filter_input(self#refill, p#really_input(str, pos));
  pub input_char = filter_input(self#refill, p#input_char);
  pub input_byte = filter_input(self#refill, p#input_byte);
  pub close_in = () => p#close_in();
  pub pos_in = p#pos_in;
};
