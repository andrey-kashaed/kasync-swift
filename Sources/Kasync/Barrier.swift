//
// DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
//
// Copyright (c) 2023 Andrey Kashaed. All rights reserved.
//
// The contents of this file are subject to the terms of the
// Common Development and Distribution License 1.0 (the "License").
// You may not use this file except in compliance with the License.
//
// You can obtain a copy of the License at
// https://opensource.org/licenses/CDDL-1.0 or LICENSE.txt.
//

import Foundation

public class Barrier {
    
    public enum Mode {
        case auto, manual
    }
    
    private let partiesCount: Int
    private let mode: Mode
    private var countdown: Int = 0
    private var continuations: [CheckedContinuation<Void, Error>] = []
    private var enabled = true
    private let lock = NSRecursiveLock()
    
    public init(partiesCount: Int = Int.max, mode: Mode = .auto) {
        self.partiesCount = partiesCount
        self.mode = mode
    }
    
    public func await() async throws {
        if lock.synchronized({ !enabled }) { return }
        try await withCancellableCheckedThrowingContinuation() { [weak self] (continuation: CheckedContinuation<Void, Error>, cancellation: Cancellation) -> Void in
            cancellation.onCancel = {
                self?.reset()
            }
            self?.addContinuation(continuation)
            self?.dispatchCountdown()
        }
    }
    
    public func signal() {
        lock.synchronized {
            countdown = partiesCount
            dispatchCountdown()
        }
    }
    
    public func reset(error: Error = CancellationError()) {
        lock.synchronized {
            for continuation in continuations {
                continuation.resume(throwing: error)
            }
            continuations.removeAll()
            countdown = 0
            enabled = true
        }
    }
    
    private func addContinuation(_ continuation: CheckedContinuation<Void, Error>) {
        lock.synchronized {
            continuations.append(continuation)
            countdown += 1
        }
    }
    
    private func dispatchCountdown() {
        lock.synchronized {
            if countdown < partiesCount { return }
            for continuation in continuations {
                continuation.resume()
            }
            continuations.removeAll()
            countdown = 0
            if mode == .manual {
                enabled = false
            }
        }
    }
    
}
